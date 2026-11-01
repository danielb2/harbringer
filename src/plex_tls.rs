use base64::Engine as _;
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, PKCS_RSA_SHA256};
use rustls::{pki_types::PrivateKeyDer, ServerConfig};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{BufReader, Write},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::Mutex;
use tokio_rustls::TlsAcceptor;

const CERT_RENEW_AFTER: Duration = Duration::from_secs(300 * 24 * 60 * 60);
const CERT_REQUEST_BACKOFF: Duration = Duration::from_secs(24 * 60 * 60);
const CERT_POLL_INTERVAL: Duration = Duration::from_secs(3);
const CERT_POLL_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct StoredCertificateState {
    hash: String,
    issued_at: u64,
    last_attempt: u64,
    last_error: Option<String>,
}

#[derive(Clone)]
pub struct PlexTls {
    directory: Arc<PathBuf>,
    server_config: Arc<RwLock<Option<Arc<ServerConfig>>>>,
    hash: Arc<RwLock<Option<String>>>,
    ensure_lock: Arc<Mutex<()>>,
}

impl PlexTls {
    pub fn load(directory: PathBuf) -> Result<Self, String> {
        fs::create_dir_all(&directory)
            .map_err(|error| format!("could not create Plex certificate directory: {error}"))?;
        let this = Self {
            directory: Arc::new(directory),
            server_config: Arc::new(RwLock::new(None)),
            hash: Arc::new(RwLock::new(None)),
            ensure_lock: Arc::new(Mutex::new(())),
        };
        this.load_stored()?;
        Ok(this)
    }

    pub fn hash(&self) -> Option<String> {
        self.hash.read().ok().and_then(|value| value.clone())
    }

    fn tls_config(&self) -> Option<Arc<ServerConfig>> {
        self.server_config
            .read()
            .ok()
            .and_then(|value| value.clone())
    }

    fn load_stored(&self) -> Result<(), String> {
        let cert_path = self.directory.join("plexdirect.crt");
        let key_path = self.directory.join("plexdirect.key");
        let state_path = self.directory.join("plexdirect.json");
        if !cert_path.exists() || !key_path.exists() || !state_path.exists() {
            return Ok(());
        }
        let state: StoredCertificateState = serde_json::from_slice(
            &fs::read(state_path)
                .map_err(|error| format!("could not read Plex certificate state: {error}"))?,
        )
        .map_err(|error| format!("invalid Plex certificate state: {error}"))?;
        let certificate = build_server_config(&cert_path, &key_path)?;
        if let Ok(mut config) = self.server_config.write() {
            *config = Some(Arc::new(certificate));
        }
        if let Ok(mut hash) = self.hash.write() {
            *hash = Some(state.hash);
        }
        Ok(())
    }

    pub async fn ensure_certificate(
        &self,
        client: &reqwest::Client,
        machine_identifier: &str,
        server_token: &str,
    ) -> Result<(), String> {
        let _guard = self.ensure_lock.lock().await;
        let now = unix_now()?;
        let state_path = self.directory.join("plexdirect.json");
        let mut stored = read_state(&state_path).unwrap_or_default();
        let current_hash = self.hash();
        let renew = current_hash.is_none()
            || current_hash.as_deref() != Some(stored.hash.as_str())
            || now.saturating_sub(stored.issued_at) >= CERT_RENEW_AFTER.as_secs();
        if !renew {
            return Ok(());
        }
        if stored.last_error.is_some()
            && now.saturating_sub(stored.last_attempt) < CERT_REQUEST_BACKOFF.as_secs()
        {
            return Err(format!(
                "Plex certificate request is paused after a recent failure: {}",
                stored.last_error.as_deref().unwrap_or("unknown error")
            ));
        }

        let result = request_certificate(client, machine_identifier, server_token).await;
        match result {
            Ok((hash, cert_pem, key_pem)) => {
                write_private_file(&self.directory.join("plexdirect.crt"), &cert_pem)?;
                write_private_file(&self.directory.join("plexdirect.key"), &key_pem)?;
                stored.hash = hash.clone();
                stored.issued_at = now;
                stored.last_attempt = now;
                stored.last_error = None;
                write_json_state(&state_path, &stored)?;
                let config = build_server_config(
                    &self.directory.join("plexdirect.crt"),
                    &self.directory.join("plexdirect.key"),
                )?;
                if let Ok(mut current) = self.server_config.write() {
                    *current = Some(Arc::new(config));
                }
                if let Ok(mut current_hash) = self.hash.write() {
                    *current_hash = Some(hash);
                }
                Ok(())
            }
            Err(error) => {
                stored.last_attempt = now;
                stored.last_error = Some(error.clone());
                let _ = write_json_state(&state_path, &stored);
                Err(error)
            }
        }
    }

    pub async fn serve(
        &self,
        listener: tokio::net::TcpListener,
        app: axum::Router,
    ) -> std::io::Result<()> {
        loop {
            let (stream, remote_addr) = listener.accept().await?;
            let tls = self.clone();
            let app = app.clone();
            tokio::spawn(async move {
                let stream = stream;
                let mut first_byte = [0u8; 1];
                let peek =
                    tokio::time::timeout(Duration::from_secs(10), stream.peek(&mut first_byte))
                        .await;
                let Ok(Ok(1)) = peek else {
                    return;
                };
                if first_byte[0] == 0x16 {
                    let Some(config) = tls.tls_config() else {
                        return;
                    };
                    match TlsAcceptor::from(config).accept(stream).await {
                        Ok(tls_stream) => serve_http(tls_stream, app, remote_addr).await,
                        Err(error) => tracing::debug!(%error, "Plex TLS handshake failed"),
                    }
                } else {
                    serve_http(stream, app, remote_addr).await;
                }
            });
        }
    }
}

async fn serve_http<S>(stream: S, app: axum::Router, remote_addr: std::net::SocketAddr)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use axum::body::Body;
    use hyper::{body::Incoming, service::service_fn};
    use hyper_util::rt::TokioIo;
    use tower::ServiceExt;

    let service = service_fn(move |mut request: hyper::Request<Incoming>| {
        let app = app.clone();
        request.extensions_mut().insert(remote_addr);
        async move { app.oneshot(request.map(Body::new)).await }
    });
    if let Err(error) = hyper::server::conn::http1::Builder::new()
        .serve_connection(TokioIo::new(stream), service)
        .with_upgrades()
        .await
    {
        tracing::debug!(%error, "Plex HTTP connection closed");
    }
}

async fn request_certificate(
    client: &reqwest::Client,
    machine_identifier: &str,
    server_token: &str,
) -> Result<(String, Vec<u8>, Vec<u8>), String> {
    let identity_headers = |request: reqwest::RequestBuilder| {
        request
            .header("X-Plex-Product", "Plex Media Server")
            .header("X-Plex-Version", "1.43.4.10903-e5521bd8c")
            .header("X-Plex-Platform", "MacOSX")
            .header("X-Plex-Platform-Version", std::env::consts::ARCH)
            .header("X-Plex-Device-Name", "harbringer")
            .header("X-Plex-Device", "MacOSX")
            .header("X-Plex-Provides", "server")
            .header("X-Plex-Client-Identifier", machine_identifier)
            .header("X-Plex-Token", server_token)
    };
    let endpoint =
        format!("https://servers.plex.tv/api/v2/devices/{machine_identifier}/certificate");
    let subject = identity_headers(client.get(format!("{endpoint}/subject")))
        .header("Accept", "application/xml")
        .send()
        .await
        .map_err(|error| format!("could not fetch Plex certificate subject: {error}"))?
        .error_for_status()
        .map_err(|error| format!("Plex refused certificate subject request: {error}"))?
        .text()
        .await
        .map_err(|error| format!("could not read Plex certificate subject: {error}"))?;
    let hash = plex_direct_hash(&subject)
        .ok_or_else(|| "Plex certificate subject did not contain a plex.direct hash".to_owned())?;
    let common_name = format!("*.{hash}.plex.direct");

    let key = KeyPair::generate_for(&PKCS_RSA_SHA256)
        .map_err(|error| format!("could not generate Plex TLS key: {error}"))?;
    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::CommonName, &common_name);
    let mut params = CertificateParams::default();
    params.distinguished_name = distinguished_name;
    let request = params
        .serialize_request(&key)
        .and_then(|request| request.pem())
        .map_err(|error| format!("could not create Plex certificate request: {error}"))?
        .replace("BEGIN CERTIFICATE REQUEST", "BEGIN NEW CERTIFICATE REQUEST")
        .replace("END CERTIFICATE REQUEST", "END NEW CERTIFICATE REQUEST");
    let key_pem = key.serialize_pem().into_bytes();
    let part = reqwest::multipart::Part::bytes(request.into_bytes())
        .mime_str("application/x-pem-file")
        .map_err(|error| format!("could not encode Plex certificate request: {error}"))?;
    identity_headers(client.put(format!("{endpoint}/csr")))
        .query(&[("reason", "missing"), ("invalidIn", "0")])
        .multipart(reqwest::multipart::Form::new().part("file", part))
        .send()
        .await
        .map_err(|error| format!("could not submit Plex certificate request: {error}"))?
        .error_for_status()
        .map_err(|error| format!("Plex rejected certificate request: {error}"))?;

    let deadline = tokio::time::Instant::now() + CERT_POLL_TIMEOUT;
    loop {
        let response = identity_headers(client.get(format!("{endpoint}/download")))
            .header(
                "Accept",
                "application/x-pem-file, application/json;q=0.9, */*;q=0.8",
            )
            .send()
            .await
            .map_err(|error| format!("could not download Plex certificate: {error}"))?;
        if response.status() == reqwest::StatusCode::ACCEPTED {
            if tokio::time::Instant::now() >= deadline {
                return Err("Plex certificate was not issued within 90 seconds".to_owned());
            }
            tokio::time::sleep(CERT_POLL_INTERVAL).await;
            continue;
        }
        let response = response
            .error_for_status()
            .map_err(|error| format!("Plex certificate download failed: {error}"))?;
        let bytes = response
            .bytes()
            .await
            .map_err(|error| format!("could not read Plex certificate: {error}"))?;
        let certificates = rustls_pemfile::certs(&mut BufReader::new(bytes.as_ref()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("Plex returned an invalid certificate chain: {error}"))?;
        if certificates.is_empty() {
            return Err("Plex returned an empty certificate chain".to_owned());
        }
        let cert_pem = certificates
            .iter()
            .map(|certificate| {
                let mut pem = String::from("-----BEGIN CERTIFICATE-----\n");
                let encoded = base64::engine::general_purpose::STANDARD.encode(certificate);
                for line in encoded.as_bytes().chunks(64) {
                    pem.push_str(std::str::from_utf8(line).unwrap_or_default());
                    pem.push('\n');
                }
                pem.push_str("-----END CERTIFICATE-----\n");
                pem
            })
            .collect::<String>()
            .into_bytes();
        return Ok((hash, cert_pem, key_pem));
    }
}

fn plex_direct_hash(subject: &str) -> Option<String> {
    let marker = "*.";
    let start = subject.find(marker)? + marker.len();
    let tail = &subject[start..];
    let end = tail.find(".plex.direct")?;
    let hash = &tail[..end];
    (!hash.is_empty()
        && hash
            .chars()
            .all(|character| character.is_ascii_alphanumeric()))
    .then(|| hash.to_ascii_lowercase())
}

fn build_server_config(cert_path: &Path, key_path: &Path) -> Result<ServerConfig, String> {
    let mut cert_reader = BufReader::new(
        File::open(cert_path)
            .map_err(|error| format!("could not open Plex certificate: {error}"))?,
    );
    let certificates = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("could not parse Plex certificate: {error}"))?;
    let mut key_reader = BufReader::new(
        File::open(key_path)
            .map_err(|error| format!("could not open Plex private key: {error}"))?,
    );
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|error| format!("could not parse Plex private key: {error}"))?
        .ok_or_else(|| "Plex private key file is empty".to_owned())?;
    ServerConfig::builder_with_provider(Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|error| format!("could not configure Plex TLS versions: {error}"))?
        .with_no_client_auth()
        .with_single_cert(certificates, key)
        .map_err(|error| format!("Plex certificate and private key do not match: {error}"))
}

fn read_state(path: &Path) -> Option<StoredCertificateState> {
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

fn write_json_state(path: &Path, state: &StoredCertificateState) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(state)
        .map_err(|error| format!("could not encode Plex certificate state: {error}"))?;
    write_private_file(path, &bytes)
}

fn write_private_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut temporary_name = path
        .file_name()
        .ok_or_else(|| "Plex certificate path has no file name".to_owned())?
        .to_os_string();
    temporary_name.push(".tmp");
    let temporary = path.with_file_name(temporary_name);
    let mut file = File::create(&temporary)
        .map_err(|error| format!("could not write Plex certificate file: {error}"))?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|error| format!("could not persist Plex certificate file: {error}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))
            .map_err(|error| format!("could not secure Plex certificate file: {error}"))?;
    }
    fs::rename(&temporary, path)
        .map_err(|error| format!("could not install Plex certificate file: {error}"))
}

fn unix_now() -> Result<u64, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))
}

#[cfg(test)]
mod tests {
    use super::plex_direct_hash;

    #[test]
    fn extracts_plex_direct_hash_from_plex_subject_xml() {
        assert_eq!(
            plex_direct_hash(r#"<subject commonName="*.abc123.plex.direct"/>"#),
            Some("abc123".to_owned())
        );
    }

    #[test]
    fn rejects_non_plex_direct_certificate_subjects() {
        assert_eq!(plex_direct_hash("CN=server.example.com"), None);
    }
}
