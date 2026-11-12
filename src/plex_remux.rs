use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
use matroska_demuxer::{Frame, MatroskaFile, TrackType};
use oxideav_aac::encoder::{EncoderConfig as AacEncoderConfig, StreamEncoder as AacStreamEncoder};
use oxideav_ac3::decoder::make_eac3_decoder;
use oxideav_core::{
    CodecId, CodecParameters, Decoder, Frame as DecodedFrame, Packet, SampleFormat, TimeBase,
};
use rust_h265::{parse_hvcc, parse_nal, Decoder as HevcDecoder, PixelData};
use rusty_h264_common::YuvFrame;
use rusty_h264_encoder::{Encoder as H264Encoder, EncoderConfig as H264EncoderConfig, Preset};
use tempfile::TempDir;
use transmux::{
    avc_config::{AVCConfigurationBox, AVCDecoderConfigurationRecord},
    nalu_types::{AvcPps, AvcSps},
    CodecConfig, Ec3SpecificBox, Ec3SyncframeInfo, HEVCConfigurationBox, Sample, Segmenter,
    TrackId, TrackSpec,
};

mod mkv_hls;

const VIDEO_TRACK_ID: u32 = 1;
const AUDIO_TRACK_ID: u32 = 2;
const VIDEO_TIMESCALE: u32 = 90_000;
const SEGMENT_TARGET_SECONDS: f64 = 2.0;
const AAC_FRAME_SAMPLES: usize = 1024;
const AAC_CHANNELS: usize = 2;
const AAC_BOUNDED_CHUNK_FRAMES: usize = 94;
const AAC_BOUNDED_CHUNK_SAMPLES: usize = AAC_FRAME_SAMPLES * AAC_CHANNELS * AAC_BOUNDED_CHUNK_FRAMES;

const MAX_PENDING_VIDEO_TICKS: i64 = VIDEO_TIMESCALE as i64 * 30;
const BUILD_IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_SEGMENTS_AHEAD: usize = 3;

#[derive(Clone)]
struct SegmentInfo {
    duration: f64,
    size: u64,
}

#[derive(Clone, Copy)]

struct VideoFrameTiming {
    pts: i64,
    duration: u32,
}


#[derive(Clone, Copy)]
struct AudioFrameTiming {
    pts: i64,
    duration: u32,
}


#[derive(Clone, Copy)]
struct AudioSourcePacket {
    pts: i64,
    sample_offset: u64,
}

struct AudioScan {
    timings: Vec<AudioFrameTiming>,
    source_packets: Vec<AudioSourcePacket>,
    sample_rate: u32,
    channels: u16,
    codec_delay: u64,
    timestamp_scale: u64,
}

struct SourceAudioTrack {
    number: u64,
    codec_id: String,
    codec_private: Vec<u8>,
    sample_rate: u32,
    packet_timescale: u32,
    channels: u16,
    codec_delay: Option<u64>,
    seek_pre_roll: Option<u64>,
}


struct BuiltSegments {
    video: Vec<SegmentInfo>,
    audio: Vec<SegmentInfo>,
    video_start_segment: usize,
}

struct VideoScan {
    segment_durations: Vec<f64>,
    segment_start_ticks: Vec<u64>,
    segment_start_pts: Vec<i64>,
    frame_timings: Vec<VideoFrameTiming>,
    codec: String,
    codec_id: String,
    width: u16,
    height: u16,
}

#[derive(Default)]
struct PlaybackProgressState {
    complete: bool,
    error: Option<String>,
    requested_video_segment: Option<usize>,
    requested_audio_segment: Option<usize>,
    requested_muxed_segment: Option<usize>,
    first_requested_muxed_segment: Option<usize>,
}

struct PlaybackProgress {
    state: Mutex<PlaybackProgressState>,
    changed: tokio::sync::Notify,
    demand_changed: Condvar,
    last_request: Mutex<Instant>,
}

impl Default for PlaybackProgress {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            changed: tokio::sync::Notify::new(),
            demand_changed: Condvar::new(),
            last_request: Mutex::new(Instant::now()),
        }
    }
}

impl PlaybackProgress {
    fn touch(&self) {
        if let Ok(mut last_request) = self.last_request.lock() {
            *last_request = Instant::now();
        }
    }

    fn is_idle_for(&self, timeout: Duration) -> bool {
        self.last_request
            .lock()
            .map(|last_request| last_request.elapsed() >= timeout)
            .unwrap_or(true)
    }

    fn request_resource(&self, name: &str, separate_dash_tracks: bool) -> Result<(), String> {
        self.touch();
        let filename = name.rsplit('/').next().unwrap_or(name);
        let segment_name = filename.strip_prefix("segment-").unwrap_or(filename);
        let Some(index) = segment_name
            .strip_suffix(".m4s")
            .or_else(|| segment_name.strip_suffix(".mkv"))
            .and_then(|index| index.parse::<usize>().ok())
        else {
            return Ok(());
        };
        let requested_segment = if separate_dash_tracks {
            let track = name
                .split_once('/')
                .map(|(track, _)| track)
                .unwrap_or_default();
            match track {
                "0" => 0,
                "1" => 1,
                _ => return Ok(()),
            }
        } else {
            2
        };
        let mut state = self
            .state
            .lock()
            .map_err(|_| "playback progress lock failed".to_owned())?;
        let requested = match requested_segment {
            0 => &mut state.requested_video_segment,
            1 => &mut state.requested_audio_segment,
            _ => &mut state.requested_muxed_segment,
        };
        *requested = Some((*requested).map_or(index, |previous| previous.max(index)));
        if requested_segment == 2 {
            state.first_requested_muxed_segment.get_or_insert(index);
        }
        self.demand_changed.notify_all();
        Ok(())
    }

    fn wait_for_capacity(
        &self,
        video_segments: usize,
        audio_segments: usize,
        video_start_segment: usize,
        audio_start_segment: usize,
        separate_dash_tracks: bool,
        has_audio: bool,
        idle_timeout: Duration,
        limiter: &Arc<tokio::sync::Semaphore>,
        permit: &mut Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<(), String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "playback progress lock failed".to_owned())?;
        loop {
            let at_capacity = if separate_dash_tracks {
                segment_window_is_full(
                    video_segments,
                    state.requested_video_segment,
                    video_start_segment,
                ) || (has_audio
                    && segment_window_is_full(
                        audio_segments,
                        state.requested_audio_segment,
                        audio_start_segment,
                    ))
            } else {
                segment_window_is_full(
                    video_segments,
                    state.requested_muxed_segment,
                    video_start_segment,
                )
            };
            if !at_capacity {
                drop(state);
                if permit.is_none() {
                    *permit = Some(
                        Arc::clone(limiter)
                            .blocking_acquire_owned()
                            .map_err(|_| "playback build limiter closed".to_owned())?,
                    );
                }
                return Ok(());
            }
            if self.is_idle_for(idle_timeout) {
                return Err("playback build stopped after the client became idle".to_owned());
            }
            drop(permit.take());
            let elapsed = self
                .last_request
                .lock()
                .map_err(|_| "playback request timer lock failed".to_owned())?
                .elapsed();
            let wait = idle_timeout.saturating_sub(elapsed);
            let (next_state, _) = self
                .demand_changed
                .wait_timeout(state, wait)
                .map_err(|_| "playback progress lock failed".to_owned())?;
            state = next_state;
        }
    }


    fn wait_for_initial_muxed_segment(
        &self,
        idle_timeout: Duration,
        limiter: &Arc<tokio::sync::Semaphore>,
        permit: &mut Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<usize, String> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "playback progress lock failed".to_owned())?;
        loop {
            if let Some(index) = state.first_requested_muxed_segment {
                drop(state);
                if permit.is_none() {
                    *permit = Some(
                        Arc::clone(limiter)
                            .blocking_acquire_owned()
                            .map_err(|_| "playback build limiter closed".to_owned())?,
                    );
                }
                return Ok(index);
            }
            if self.is_idle_for(idle_timeout) {
                return Err("playback build stopped after the client became idle".to_owned());
            }
            drop(permit.take());
            let elapsed = self
                .last_request
                .lock()
                .map_err(|_| "playback request timer lock failed".to_owned())?
                .elapsed();
            let wait = idle_timeout.saturating_sub(elapsed);
            let (next_state, _) = self
                .demand_changed
                .wait_timeout(state, wait)
                .map_err(|_| "playback progress lock failed".to_owned())?;
            state = next_state;
        }
    }
}

fn segment_window_is_full(
    generated: usize,
    requested: Option<usize>,
    start_segment: usize,
) -> bool {
    generated
        >= requested
            .unwrap_or(start_segment)
            .max(start_segment)
            .saturating_add(MAX_SEGMENTS_AHEAD)
            .saturating_add(1)
}

pub struct RemuxedPlayback {
    directory: TempDir,
    segments: Vec<SegmentInfo>,
    audio_segments: Vec<SegmentInfo>,
    progress: Arc<PlaybackProgress>,
    video_codec: String,
    video_codec_id: String,
    audio_codec: Option<String>,
    width: u16,
    height: u16,
    transcode_audio: bool,
    video_timings: Vec<VideoFrameTiming>,
    audio_timings: Vec<AudioFrameTiming>,
    audio_sample_rate: Option<u32>,
    audio_channels: Option<u16>,
    separate_dash_tracks: bool,
    matroska_hls: bool,
    video_transcode: Option<VideoTranscodeConfig>,
    video_start_segment: usize,
    video_segment_start_pts: Vec<i64>,
    video_segment_start_ticks: Vec<u64>,
    audio_start_segment: usize,
    video_start_pts: i64,
    video_decode_start_pts: i64,
    audio_start_pts: i64,
    audio_decode_start_pts: i64,
    audio_source_seek_pts: i64,
    source_seek_timestamp: u64,
    should_seek_source: bool,
}

impl RemuxedPlayback {
    #[cfg(test)]
    pub fn build(&self, path: &Path) -> Result<(), String> {
        let limiter = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::clone(&limiter)
            .try_acquire_owned()
            .map_err(|_| "playback build limiter unavailable".to_owned())?;
        self.build_with_permit(path, limiter, permit)
    }


    pub fn prepare(
        path: &Path,
        transcode_audio: bool,
        transcode_video: bool,
        separate_dash_tracks: bool,
        matroska_hls: bool,
        offset_seconds: f64,
    ) -> Result<Self, String> {
        let directory = tempfile::Builder::new()
            .prefix("harbringer-playback-")
            .tempdir()
            .map_err(|error| format!("create playback cache: {error}"))?;
        let scan = Self::scan_video_segments(path)?;
        if transcode_video && scan.codec_id != "V_MPEGH/ISO/HEVC" {
            return Err("video transcoding currently requires an HEVC source".to_owned());
        }
        if scan.segment_durations.is_empty() {
            return Err("media did not contain a complete playback segment".to_owned());
        }
        let audio_codec = Self::validate_source(path, transcode_audio, matroska_hls)?;
        let source_size = fs::metadata(path).map_err(media_error)?.len();
        let source_duration = scan.segment_durations.iter().sum::<f64>();
        if !offset_seconds.is_finite() || offset_seconds < 0.0 || offset_seconds > source_duration {
            return Err("playback offset is outside the source duration".to_owned());
        }
        let target_bitrate = ((source_size as f64 * 8.0 / source_duration) * 1.5)
            .clamp(2_000_000.0, 12_000_000.0) as u32;
        let video_transcode = if transcode_video {
            Some(VideoTranscodeConfig::prepare(
                scan.width,
                scan.height,
                &scan.frame_timings,
                target_bitrate,
            )?)
        } else {
            None
        };
        let segment_durations = if let Some(config) = &video_transcode {
            transcode_segment_durations(&scan.frame_timings, config.gop_size)?
        } else {
            scan.segment_durations.clone()
        };
        let (should_seek_source, video_start_segment) =
            playback_segment_start(&segment_durations, offset_seconds);
        let video_start_pts = if let Some(config) = &video_transcode {
            let frame_index = video_start_segment
                .checked_mul(usize::try_from(config.gop_size).map_err(media_error)?)
                .ok_or_else(|| "playback segment index overflow".to_owned())?;
            scan.frame_timings
                .get(frame_index)
                .ok_or_else(|| "playback offset has no matching video frame".to_owned())?
                .pts
        } else {
            scan.segment_start_pts[video_start_segment]
        };
        let video_decode_segment = if should_seek_source && video_transcode.is_some() {
            segment_index_for_offset(&scan.segment_durations, (offset_seconds - 2.0).max(0.0))
        } else {
            video_start_segment
        };
        let video_decode_start_pts = scan.segment_start_pts[video_decode_segment];
        let video_source_seek = scan.segment_start_ticks[video_decode_segment];
        let total_duration = segment_durations.iter().sum::<f64>();
        let segments = segment_durations
            .into_iter()
            .map(|duration| SegmentInfo {
                duration,
                size: video_transcode.as_ref().map_or_else(
                    || (source_size as f64 * duration / total_duration).ceil() as u64,
                    |config| ((f64::from(config.bitrate) + 128_000.0) * duration / 8.0).ceil() as u64,
                ),
            })
            .collect();
        let audio_scan = if separate_dash_tracks {
            scan_audio_timings(path, transcode_audio)?
        } else {
            None
        };
        let audio_durations = audio_scan.as_ref().map_or_else(Vec::new, |scan| {
            audio_segment_durations(&scan.timings, scan.sample_rate)
        });
        let audio_start_segment = if should_seek_source && !audio_durations.is_empty() {
            segment_index_for_offset(&audio_durations, offset_seconds)
        } else {
            0
        };
        let audio_start_pts = audio_scan.as_ref().map_or(0, |scan| {
            audio_segment_start_pts(&scan.timings, scan.sample_rate, audio_start_segment)
                .unwrap_or_default()
        });
        let (audio_source_seek_pts, audio_decode_start_pts) =
            if let Some(scan) = audio_scan.as_ref() {
                if should_seek_source && transcode_audio {
                    audio_transcode_seek_point(scan, audio_start_pts)?
                } else {
                    (audio_start_pts, audio_start_pts)
                }
            } else {
                (audio_start_pts, audio_start_pts)
            };
        let audio_source_seek = if let Some(scan) = audio_scan.as_ref() {
            audio_pts_to_matroska_ticks(
                audio_source_seek_pts,
                scan.sample_rate,
                scan.codec_delay,
                scan.timestamp_scale,
            )?
        } else {
            video_source_seek
        };
        let source_seek_timestamp = if audio_codec.is_some() {
            video_source_seek.min(audio_source_seek)
        } else {
            video_source_seek
        };
        let bitrate = if transcode_audio { 128_000.0 } else { 768_000.0 };
        let audio_segments = audio_durations
            .into_iter()
            .map(|duration| SegmentInfo {
                duration,
                size: (bitrate * duration / 8.0).ceil() as u64,
            })
            .collect::<Vec<_>>();
        if separate_dash_tracks && audio_codec.is_some() && audio_segments.is_empty() {
            return Err("audio track did not contain a complete DASH segment".to_owned());
        }
        let video_codec = video_transcode
            .as_ref()
            .map(|config| config.codec.clone())
            .unwrap_or_else(|| scan.codec.clone());
        Ok(Self {
            directory,
            segments,
            audio_segments,
            progress: Arc::default(),
            video_codec,
            video_codec_id: scan.codec_id.clone(),
            audio_codec,
            width: scan.width,
            height: scan.height,
            transcode_audio,
            video_timings: scan.frame_timings,
            audio_timings: audio_scan.as_ref().map_or_else(Vec::new, |scan| scan.timings.clone()),
            audio_sample_rate: audio_scan.as_ref().map(|scan| scan.sample_rate),
            audio_channels: audio_scan.as_ref().map(|scan| scan.channels),
            separate_dash_tracks,
            matroska_hls,
            video_transcode,
            video_start_segment,
            video_segment_start_pts: scan.segment_start_pts.clone(),
            video_segment_start_ticks: scan.segment_start_ticks.clone(),
            audio_start_segment,
            video_start_pts,
            video_decode_start_pts,
            audio_start_pts,
            audio_decode_start_pts,
            audio_source_seek_pts,
            source_seek_timestamp,
            should_seek_source,
        })
    }

    #[cfg(test)]
    pub fn remux_with_audio_transcoding(path: &Path, transcode_audio: bool) -> Result<Self, String> {
        let playback = Self::prepare(path, transcode_audio, false, false, false, 0.0)?;
        playback.build(path)?;
        Ok(playback)
    }

    pub fn build_with_permit(
        &self,
        path: &Path,
        limiter: Arc<tokio::sync::Semaphore>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<(), String> {
        let permit = std::cell::RefCell::new(Some(permit));
        let result = remux_matroska(
            path,
            self.directory.path(),
            self.transcode_audio,
            self.video_transcode.as_ref(),
            &self.video_codec_id,
            &self.video_timings,
            &self.video_segment_start_pts,
            &self.video_segment_start_ticks,
            &self.audio_timings,
            self.audio_sample_rate,
            self.separate_dash_tracks,
            self.matroska_hls,
            &self.segments,
            self.video_start_segment,
            self.audio_start_segment,
            self.video_start_pts,
            self.video_decode_start_pts,
            self.audio_start_pts,
            self.audio_decode_start_pts,
            self.audio_source_seek_pts,
            self.source_seek_timestamp,
            self.should_seek_source,
            &mut || {
                if self.separate_dash_tracks || self.video_transcode.is_some() {
                    return Ok(None);
                }
                let mut permit = permit.borrow_mut();
                self.progress
                    .wait_for_initial_muxed_segment(BUILD_IDLE_TIMEOUT, &limiter, &mut permit)
                    .map(Some)
            },
            &mut |video_segments, audio_segments, video_start_segment, audio_start_segment| {
                self.progress.wait_for_capacity(
                    video_segments,
                    audio_segments,
                    video_start_segment,
                    audio_start_segment,
                    self.separate_dash_tracks,
                    self.audio_codec.is_some(),
                    BUILD_IDLE_TIMEOUT,
                    &limiter,
                    &mut permit.borrow_mut(),
                )
            },
            &mut |_name| self.progress.changed.notify_waiters(),
        );
        let result = result.and_then(|mut actual| {
            actual.video[..actual.video_start_segment]
                .clone_from_slice(&self.segments[..actual.video_start_segment]);
            actual.audio[..self.audio_start_segment]
                .clone_from_slice(&self.audio_segments[..self.audio_start_segment]);
            if !segment_timelines_match(&actual.video, &self.segments)
                || !segment_timelines_match(&actual.audio, &self.audio_segments)
            {
                return Err("precomputed segment timelines did not match remux output".to_owned());
            }
            Ok(())
        });
        if let Ok(mut state) = self.progress.state.lock() {
            state.complete = true;
            state.error = result.as_ref().err().cloned();
        }
        self.progress.changed.notify_waiters();
        result
    }


    pub fn has_failed(&self) -> bool {
        self.progress
            .state
            .lock()
            .map(|state| state.error.is_some())
            .unwrap_or(true)
    }





    pub async fn wait_resource(&self, name: &str) -> Result<PathBuf, String> {
        let path = self
            .resource(name)
            .ok_or_else(|| "unknown playback resource".to_owned())?;
        self.progress
            .request_resource(name, self.separate_dash_tracks)?;
        loop {
            let notified = self.progress.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if path.is_file() {
                return Ok(path);
            }
            let (error, complete) = {
                let state = self
                    .progress
                    .state
                    .lock()
                    .map_err(|_| "playback progress lock failed".to_owned())?;
                (state.error.clone(), state.complete)
            };
            if let Some(error) = error {
                return Err(error);
            }
            if complete {
                return Err("playback resource was not generated".to_owned());
            }
            notified.await;
        }
    }


    pub fn matroska_hls(&self) -> bool {
        self.matroska_hls
    }

    pub fn resource(&self, name: &str) -> Option<PathBuf> {
        if self.separate_dash_tracks {
            let (track, filename) = if let Some(filename) = name.strip_prefix("0/") {
                (0, filename)
            } else if let Some(filename) = name.strip_prefix("1/") {
                (1, filename)
            } else {
                return None;
            };
            if filename == "header" {
                return match track {
                    0 => Some(self.directory.path().join("init-video.mp4")),
                    1 if self.audio_codec.is_some() => {
                        Some(self.directory.path().join("init-audio.mp4"))
                    }
                    _ => None,
                };
            }
            let index = filename
                .strip_prefix("segment-")
                .unwrap_or(filename)
                .strip_suffix(".m4s")?
                .parse::<usize>()
                .ok()?;
            return match track {
                0 if index >= self.video_start_segment && index < self.segments.len() => Some(
                    self.directory
                        .path()
                        .join(format!("video-segment-{index}.m4s")),
                ),
                1 if index >= self.audio_start_segment && index < self.audio_segments.len() => Some(
                    self.directory
                        .path()
                        .join(format!("audio-segment-{index}.m4s")),
                ),
                _ => None,
            };
        }

        if self.matroska_hls {
            if name == "init.mkv" {
                return Some(self.directory.path().join("init.mkv"));
            }
            let index = name.strip_suffix(".mkv")?.strip_prefix("segment-")?.parse::<usize>().ok()?;
            return (index >= self.video_start_segment && index < self.segments.len())
                .then(|| self.directory.path().join(format!("segment-{index}.mkv")));
        }
        if name == "init.mp4" || name == "0/header" {
            return Some(self.directory.path().join("init.mp4"));
        }
        let filename = name.strip_prefix("0/").unwrap_or(name);
        let index = filename
            .strip_prefix("segment-")
            .unwrap_or(filename)
            .strip_suffix(".m4s")?
            .parse::<usize>()
            .ok()?;
        (index < self.segments.len())
            .then(|| self.directory.path().join(format!("segment-{index}.m4s")))
    }

    fn validate_source(
        path: &Path,
        transcode_audio: bool,
        matroska_hls: bool,
    ) -> Result<Option<String>, String> {
        let source =
            MatroskaFile::open(File::open(path).map_err(media_error)?).map_err(media_error)?;
        let audio = source.tracks().iter().find(|track| {
            track.track_type() == TrackType::Audio
                && (track.codec_id() == "A_EAC3"
                    || (matroska_hls && !transcode_audio && track.codec_id() == "A_OPUS"))
        });
        if source
            .tracks()
            .iter()
            .any(|track| track.track_type() == TrackType::Audio)
            && audio.is_none()
        {
            let codecs = source
                .tracks()
                .iter()
                .filter(|track| track.track_type() == TrackType::Audio)
                .map(|track| track.codec_id())
                .collect::<Vec<_>>();
            return Err(format!(
                "source audio codec is not supported for remuxing; audio CodecIDs: {codecs:?}"
            ));
        }
        match audio {
            Some(track) if track.codec_id() == "A_OPUS" => {
                let settings = track
                    .audio()
                    .ok_or_else(|| "Opus track has no audio settings".to_owned())?;
                let codec_private = track
                    .codec_private()
                    .ok_or_else(|| "Opus track has no OpusHead".to_owned())?;
                let (_, _, codec_delay) = opus_track_config(
                    codec_private,
                    settings.sampling_frequency().round() as u32,
                    u16::try_from(settings.channels().get()).map_err(media_error)?,
                )?;
                if track
                    .codec_delay()
                    .is_some_and(|delay| delay != codec_delay)
                {
                    return Err("Opus CodecDelay does not match OpusHead pre-skip".to_owned());
                }
                Ok(Some("opus".to_owned()))
            }
            Some(track) if transcode_audio => {
                let sample_rate = track
                    .audio()
                    .ok_or_else(|| "E-AC-3 track has no audio settings".to_owned())?
                    .sampling_frequency()
                    .round() as u32;
                Eac3AacTranscoder::new(sample_rate, None, false)?;
                Ok(Some("mp4a.40.2".to_owned()))
            }
            Some(_) => Ok(Some("ec-3".to_owned())),
            None if transcode_audio => Err("source has no E-AC-3 audio track".to_owned()),
            None => Ok(None),
        }
    }

    pub fn hls_manifest(&self, session_id: &str) -> String {
        let target_duration = self
            .segments
            .iter()
            .map(|segment| segment.duration)
            .fold(1.0_f64, f64::max)
            .ceil() as u64;
        let mut output = format!(
            "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:{target_duration}\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-MAP:URI=\"/video/:/transcode/universal/session/{session_id}/{}\"\n",
            if self.matroska_hls { "init.mkv" } else { "init.mp4" }

        );
        for (index, segment) in self.segments.iter().enumerate() {
            let resource = if self.matroska_hls {
                format!("segment-{index}.mkv")
            } else {
                format!("segment-{index}.m4s")
            };
            output.push_str(&format!("#EXTINF:{:.3},\n/video/:/transcode/universal/session/{session_id}/{resource}\n", segment.duration));
        }
        output.push_str("#EXT-X-ENDLIST\n");
        output
    }

    pub fn dash_manifest(&self, session_id: &str) -> String {
        if self.separate_dash_tracks {
            return self.separate_dash_manifest(session_id);
        }
        let duration = self
            .segments
            .iter()
            .map(|segment| segment.duration)
            .sum::<f64>();
        let bandwidth = self
            .segments
            .iter()
            .map(|segment| (segment.size as f64 * 8.0 / segment.duration).ceil() as u64)
            .max()
            .unwrap_or(1);
        let codecs = self
            .audio_codec
            .as_ref()
            .map(|audio| format!("{},{}", self.video_codec, audio))
            .unwrap_or_else(|| self.video_codec.clone());
        let mut output = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><MPD xmlns=\"urn:mpeg:dash:schema:mpd:2011\" type=\"static\" profiles=\"urn:mpeg:dash:profile:isoff-live:2011\" minBufferTime=\"PT1.5S\" mediaPresentationDuration=\"PT{duration:.3}S\"><Period duration=\"PT{duration:.3}S\"><AdaptationSet mimeType=\"video/mp4\" segmentAlignment=\"true\" startWithSAP=\"1\"><Representation id=\"0\" bandwidth=\"{bandwidth}\" codecs=\"{codecs}\" width=\"{width}\" height=\"{height}\"><SegmentTemplate timescale=\"1000\" initialization=\"session/{session_id}/0/header\" media=\"session/{session_id}/0/$Number$.m4s\" startNumber=\"0\"><SegmentTimeline>",
            width = self.width,
            height = self.height,
        );
        append_segment_timeline(&mut output, &self.segments);
        output.push_str(
            "</SegmentTimeline></SegmentTemplate></Representation></AdaptationSet></Period></MPD>",
        );
        output
    }

    fn separate_dash_manifest(&self, session_id: &str) -> String {
        let video_duration = self.segments.iter().map(|segment| segment.duration).sum::<f64>();
        let audio_duration = self
            .audio_segments
            .iter()
            .map(|segment| segment.duration)
            .sum::<f64>();
        let duration = video_duration.max(audio_duration);
        let video_bandwidth = segment_bandwidth(&self.segments);
        let mut output = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><MPD xmlns=\"urn:mpeg:dash:schema:mpd:2011\" type=\"static\" profiles=\"urn:mpeg:dash:profile:isoff-live:2011\" minBufferTime=\"PT1.5S\" mediaPresentationDuration=\"PT{duration:.3}S\"><Period duration=\"PT{duration:.3}S\"><AdaptationSet id=\"0\" contentType=\"video\" mimeType=\"video/mp4\" segmentAlignment=\"false\" startWithSAP=\"1\"><Representation id=\"video\" bandwidth=\"{video_bandwidth}\" codecs=\"{video_codec}\" width=\"{width}\" height=\"{height}\"><SegmentTemplate timescale=\"1000\" initialization=\"session/{session_id}/0/header\" media=\"session/{session_id}/0/$Number$.m4s\" startNumber=\"0\"><SegmentTimeline>",
            video_codec = self.video_codec,
            width = self.width,
            height = self.height,
        );
        append_segment_timeline(&mut output, &self.segments);
        output.push_str("</SegmentTimeline></SegmentTemplate></Representation></AdaptationSet>");
        if let (Some(audio_codec), Some(sample_rate), Some(channels)) = (
            self.audio_codec.as_deref(),
            self.audio_sample_rate,
            self.audio_channels,
        ) {
            let audio_bandwidth = segment_bandwidth(&self.audio_segments);
            output.push_str(&format!(
                "<AdaptationSet id=\"1\" contentType=\"audio\" mimeType=\"audio/mp4\" segmentAlignment=\"false\"><Representation id=\"audio\" bandwidth=\"{audio_bandwidth}\" codecs=\"{audio_codec}\" audioSamplingRate=\"{sample_rate}\"><AudioChannelConfiguration schemeIdUri=\"urn:mpeg:dash:23003:3:audio_channel_configuration:2011\" value=\"{channels}\"/><SegmentTemplate timescale=\"1000\" initialization=\"session/{session_id}/1/header\" media=\"session/{session_id}/1/$Number$.m4s\" startNumber=\"0\"><SegmentTimeline>"
            ));
            append_segment_timeline(&mut output, &self.audio_segments);
            output.push_str("</SegmentTimeline></SegmentTemplate></Representation></AdaptationSet>");
        }
        output.push_str("</Period></MPD>");
        output
    }

    fn scan_video_segments(path: &Path) -> Result<VideoScan, String> {
        let mut source =
            MatroskaFile::open(File::open(path).map_err(media_error)?).map_err(media_error)?;
        let timestamp_scale = source.info().timestamp_scale().get();
        let video = source
            .tracks()
            .iter()
            .find(|track| {
                track.track_type() == TrackType::Video
                    && matches!(track.codec_id(), "V_MPEGH/ISO/HEVC" | "V_MPEG4/ISO/AVC")
            })
            .ok_or_else(|| {
                let codecs = source
                    .tracks()
                    .iter()
                    .filter(|track| track.track_type() == TrackType::Video)
                    .map(|track| track.codec_id())
                    .collect::<Vec<_>>();
                format!("source has no remux-supported video track; video CodecIDs: {codecs:?}")
            })?;
        let video_number = video.track_number().get();
        let video_codec_id = video.codec_id().to_owned();
        let video_private = video
            .codec_private()
            .ok_or_else(|| "video track has no decoder configuration".to_owned())?;
        let (codec, nal_length_size) = match video_codec_id.as_str() {
            "V_MPEGH/ISO/HEVC" => {
                let config =
                    HEVCConfigurationBox::parse_body(video_private).map_err(media_error)?;
                let length_size = usize::from(
                    *video_private
                        .get(21)
                        .ok_or_else(|| "HEVC decoder configuration is too short".to_owned())?
                        & 0x03,
                ) + 1;
                (config.config.rfc6381(), length_size)
            }
            "V_MPEG4/ISO/AVC" => {
                if video_private.len() < 5 {
                    return Err("AVC decoder configuration is too short".to_owned());
                }
                AVCConfigurationBox::parse_body(video_private).map_err(media_error)?;
                let codec = format!(
                    "avc1.{:02X}{:02X}{:02X}",
                    video_private[1], video_private[2], video_private[3]
                );
                (codec, usize::from(video_private[4] & 0x03) + 1)
            }
            _ => unreachable!(),
        };
        let video_settings = video
            .video()
            .ok_or_else(|| "video track has no video settings".to_owned())?;
        let width = u16::try_from(video_settings.pixel_width().get())
            .map_err(|_| "video width is outside the supported range".to_owned())?;
        let height = u16::try_from(video_settings.pixel_height().get())
            .map_err(|_| "video height is outside the supported range".to_owned())?;
        let default_duration = video.default_duration().map(|duration| duration.get());
        let codec_delay = video.codec_delay().unwrap_or(0);
        let target_ticks = (f64::from(VIDEO_TIMESCALE) * SEGMENT_TARGET_SECONDS) as u64;
        let mut frame = Frame::default();
        let mut start = None::<i64>;
        let mut pending_ticks = 0_u64;
        let mut last_video_end = None::<i64>;
        let mut durations = Vec::new();
        let mut segment_start_ticks = Vec::new();
        let mut segment_start_pts = Vec::new();
        let mut frame_timings = Vec::new();

        while source.next_frame(&mut frame).map_err(media_error)? {
            if frame.track != video_number {
                continue;
            }
            let duration_ns = frame
                .duration
                .map(|duration| duration.saturating_mul(timestamp_scale))
                .or(default_duration)
                .ok_or_else(|| "video track has no frame duration metadata".to_owned())?;
            let pts = ns_to_ticks(
                frame_timestamp_ns(frame.timestamp, timestamp_scale)?.saturating_sub(codec_delay),
                VIDEO_TIMESCALE,
            )?;
            let duration = ns_to_duration(duration_ns, VIDEO_TIMESCALE)?;
            frame_timings.push(VideoFrameTiming { pts, duration });
            let is_sync = frame.is_keyframe.unwrap_or_else(|| {
                is_video_sync_sample(&video_codec_id, &frame.data, nal_length_size)
            });
            if is_sync {
                if pending_ticks >= target_ticks {
                    if let Some(segment_start) = start {
                        durations.push(
                            ticks_to_seconds(pts.saturating_sub(segment_start), VIDEO_TIMESCALE)
                                .max(0.001),
                        );
                    } else {
                        durations.push(0.001);
                    }
                    start = Some(pts);
                    segment_start_ticks.push(frame.timestamp);
                    segment_start_pts.push(pts);
                    pending_ticks = 0;
                } else if start.is_none() {
                    start = Some(pts);
                    segment_start_ticks.push(frame.timestamp);
                    segment_start_pts.push(pts);
                }
            }
            pending_ticks = pending_ticks.saturating_add(u64::from(duration));
            last_video_end = Some(pts.saturating_add(i64::from(duration)));
        }

        let start = start.ok_or_else(|| "video track has no sync sample".to_owned())?;
        let end = last_video_end.ok_or_else(|| "video track has no frames".to_owned())?;
        durations.push(ticks_to_seconds(end.saturating_sub(start), VIDEO_TIMESCALE).max(0.001));
        frame_timings.sort_by_key(|timing| timing.pts);
        Ok(VideoScan {
            segment_durations: durations,
            segment_start_ticks,
            segment_start_pts,
            frame_timings,
            codec,
            codec_id: video_codec_id,
            width,
            height,
        })
    }
}

fn opus_track_config(
    codec_private: &[u8],
    fallback_sample_rate: u32,
    fallback_channels: u16,
) -> Result<(u32, u16, u64), String> {
    if codec_private.len() < 19 || !codec_private.starts_with(b"OpusHead") {
        return Err("invalid OpusHead codec private data".to_owned());
    }
    let channels = u16::from(codec_private[9]);
    if channels == 0 || channels != fallback_channels {
        return Err("OpusHead channel count does not match track metadata".to_owned());
    }
    let mapping_family = codec_private[18];
    if (mapping_family == 0 && channels > 2)
        || (mapping_family != 0 && codec_private.len() < 21 + usize::from(channels))
    {
        return Err("OpusHead channel mapping is incomplete".to_owned());
    }
    let input_sample_rate =
        u32::from_le_bytes(codec_private[12..16].try_into().map_err(media_error)?);
    let sample_rate = if input_sample_rate == 0 {
        fallback_sample_rate
    } else {
        input_sample_rate
    };
    if sample_rate == 0 {
        return Err("OpusHead input sample rate is missing".to_owned());
    }
    let pre_skip_samples = u16::from_le_bytes([codec_private[10], codec_private[11]]);
    let codec_delay = u64::from(pre_skip_samples) * 1_000_000_000 / 48_000;
    Ok((sample_rate, channels, codec_delay))
}


fn scan_audio_timings(path: &Path, transcode_audio: bool) -> Result<Option<AudioScan>, String> {
    let mut source = MatroskaFile::open(File::open(path).map_err(media_error)?).map_err(media_error)?;
    let timestamp_scale = source.info().timestamp_scale().get();
    let Some(track) = source
        .tracks()
        .iter()
        .find(|track| track.track_type() == TrackType::Audio && track.codec_id() == "A_EAC3")
    else {
        return Ok(None);
    };
    let number = track.track_number().get();
    let settings = track
        .audio()
        .ok_or_else(|| "E-AC-3 track has no audio settings".to_owned())?;
    let sample_rate = settings.sampling_frequency().round() as u32;
    let channels = if transcode_audio {
        2
    } else {
        u16::try_from(settings.channels().get()).map_err(media_error)?
    };
    let codec_delay = track.codec_delay().unwrap_or(0);
    let mut first_pts = None::<i64>;
    let mut decoded_samples = 0_u64;
    let mut timings = Vec::new();
    let mut source_packets = Vec::new();
    let mut frame = Frame::default();
    while source.next_frame(&mut frame).map_err(media_error)? {
        if frame.track != number {
            continue;
        }
        let pts = ns_to_ticks(
            frame_timestamp_ns(frame.timestamp, timestamp_scale)?.saturating_sub(codec_delay),
            sample_rate,
        )?;
        let syncframes = Ec3SyncframeInfo::from_es_first_au(&frame.data);
        let syncframe = syncframes
            .first()
            .ok_or_else(|| "invalid E-AC-3 sample".to_owned())?;
        if transcode_audio {
            first_pts.get_or_insert(pts);
            let packet_samples = syncframes
                .iter()
                .map(|syncframe| u64::from(syncframe.numblks) * 256)
                .sum::<u64>();
            source_packets.push(AudioSourcePacket {
                pts,
                sample_offset: decoded_samples,
            });
            decoded_samples = decoded_samples.saturating_add(packet_samples);
        } else {
            timings.push(AudioFrameTiming {
                pts,
                duration: u32::from(syncframe.numblks) * 256,
            });
        }
    }
    if transcode_audio {
        let output_frame_count = decoded_samples.div_ceil(1024).saturating_sub(1);
        let first_pts = first_pts.unwrap_or(0);
        for index in 0..output_frame_count {
            let offset = i64::try_from(index)
                .map_err(media_error)?
                .saturating_mul(1024);
            timings.push(AudioFrameTiming {
                pts: first_pts.saturating_add(offset),
                duration: 1024,
            });
        }
    }
    timings.sort_by_key(|timing| timing.pts);
    Ok(Some(AudioScan {
        timings,
        source_packets,
        sample_rate,
        channels,
        codec_delay,
        timestamp_scale,
    }))
}

fn audio_segment_durations(timings: &[AudioFrameTiming], timescale: u32) -> Vec<f64> {
    let Some(first) = timings.first() else {
        return Vec::new();
    };
    if timescale == 0 {
        return Vec::new();
    }
    let target_ticks = (f64::from(timescale) * SEGMENT_TARGET_SECONDS) as u64;
    let mut segment_start = first.pts;
    let mut pending_ticks = 0_u64;
    let mut durations = Vec::new();
    for (index, timing) in timings.iter().enumerate() {
        if index > 0 && pending_ticks >= target_ticks {
            durations.push(
                ticks_to_seconds(timing.pts.saturating_sub(segment_start), timescale).max(0.001),
            );
            segment_start = timing.pts;
            pending_ticks = 0;
        }
        pending_ticks = pending_ticks.saturating_add(u64::from(timing.duration));
    }
    let last = timings.last().expect("checked non-empty");
    let end = last.pts.saturating_add(i64::from(last.duration));
    durations.push(ticks_to_seconds(end.saturating_sub(segment_start), timescale).max(0.001));
    durations
}


fn segment_index_for_offset(durations: &[f64], offset_seconds: f64) -> usize {
    let mut start = 0.0;
    for (index, duration) in durations.iter().enumerate() {
        if offset_seconds < start + duration || index + 1 == durations.len() {
            return index;
        }
        start += duration;
    }
    0
}

fn playback_segment_start(durations: &[f64], offset_seconds: f64) -> (bool, usize) {
    if offset_seconds > 0.0 {
        (true, segment_index_for_offset(durations, offset_seconds))
    } else {
        (false, 0)
    }
}


fn hls_segment_start(
    requested_segment: usize,
    prepared_segment: usize,
    segment_start_pts: &[i64],
    segment_start_ticks: &[u64],
) -> Result<(usize, i64, u64), String> {
    let segment = requested_segment.max(prepared_segment);
    let pts = *segment_start_pts
        .get(segment)
        .ok_or_else(|| "requested HLS segment has no source start timestamp".to_owned())?;
    let ticks = *segment_start_ticks
        .get(segment)
        .ok_or_else(|| "requested HLS segment has no source seek point".to_owned())?;
    Ok((segment, pts, ticks))
}


fn should_seek_matroska_source(
    should_seek_source: bool,
    separate_dash_tracks: bool,
    transcode_video: bool,
) -> bool {
    should_seek_source && (separate_dash_tracks || transcode_video)
}

fn audio_segment_start_pts(
    timings: &[AudioFrameTiming],
    timescale: u32,
    segment_index: usize,
) -> Option<i64> {
    if timescale == 0 {
        return None;
    }
    let target_ticks = (f64::from(timescale) * SEGMENT_TARGET_SECONDS) as u64;
    let mut current_segment = 0;
    let mut pending_ticks = 0_u64;
    for (index, timing) in timings.iter().enumerate() {
        if index > 0 && pending_ticks >= target_ticks {
            current_segment += 1;
            pending_ticks = 0;
        }
        if current_segment == segment_index {
            return Some(timing.pts);
        }
        pending_ticks = pending_ticks.saturating_add(u64::from(timing.duration));
    }
    None
}


fn audio_transcode_seek_point(
    scan: &AudioScan,
    target_pts: i64,
) -> Result<(i64, i64), String> {
    let first_pts = scan.timings.first().map_or(target_pts, |timing| timing.pts);
    let target_samples = u64::try_from(target_pts.saturating_sub(first_pts).max(0))
        .map_err(media_error)?;
    let preroll_samples = u64::from(scan.sample_rate).saturating_mul(2);
    let earliest_sample = target_samples.saturating_sub(preroll_samples);
    let packet = scan
        .source_packets
        .iter()
        .rev()
        .find(|packet| packet.sample_offset <= earliest_sample && packet.sample_offset % 1024 == 0)
        .or_else(|| scan.source_packets.first())
        .ok_or_else(|| "audio track has no source packet for offset seeking".to_owned())?;
    let output_offset = i64::try_from(packet.sample_offset).map_err(media_error)?;
    Ok((packet.pts, first_pts.saturating_add(output_offset)))
}

fn audio_pts_to_matroska_ticks(
    pts: i64,
    sample_rate: u32,
    codec_delay: u64,
    timestamp_scale: u64,
) -> Result<u64, String> {
    if sample_rate == 0 || timestamp_scale == 0 {
        return Err("audio timestamp scale is invalid".to_owned());
    }
    let audio_ns = u128::try_from(pts.max(0))
        .map_err(media_error)?
        .saturating_mul(1_000_000_000)
        / u128::from(sample_rate);
    let source_ns = audio_ns.saturating_add(u128::from(codec_delay));
    u64::try_from(source_ns / u128::from(timestamp_scale)).map_err(media_error)
}

fn segment_timelines_match(actual: &[SegmentInfo], expected: &[SegmentInfo]) -> bool {
    actual.len() == expected.len()
        && actual
            .iter()
            .zip(expected)
            .all(|(actual, expected)| (actual.duration - expected.duration).abs() <= 0.002)
}


fn check_audio_timing(
    timings: &[AudioFrameTiming],
    index: usize,
    pts: i64,
    duration: u32,
) -> Result<(), String> {
    let expected = timings
        .get(index)
        .ok_or_else(|| "audio encoder produced more frames than the prepared timeline".to_owned())?;
    if expected.pts != pts || expected.duration != duration {
        return Err("audio encoder output differed from the prepared DASH timeline".to_owned());
    }
    Ok(())
}

fn segment_bandwidth(segments: &[SegmentInfo]) -> u64 {
    segments
        .iter()
        .map(|segment| (segment.size as f64 * 8.0 / segment.duration).ceil() as u64)
        .max()
        .unwrap_or(1)
}

fn append_segment_timeline(output: &mut String, segments: &[SegmentInfo]) {
    for segment in segments {
        output.push_str(&format!(
            "<S d=\"{}\"/>",
            (segment.duration * 1000.0).round() as u64
        ));
    }
}

struct VideoTranscodeConfig {
    gop_size: u32,
    bitrate: u32,
    frame_rate: f32,
    sps: Vec<u8>,
    pps: Vec<u8>,
    codec: String,
}

impl VideoTranscodeConfig {
    fn prepare(
        width: u16,
        height: u16,
        timings: &[VideoFrameTiming],
        bitrate: u32,
    ) -> Result<Self, String> {
        let frame_ticks = timings
            .iter()
            .map(|timing| u64::from(timing.duration))
            .sum::<u64>();
        if frame_ticks == 0 || width % 2 != 0 || height % 2 != 0 {
            return Err("H.264 conversion requires timed, even-sized 4:2:0 video".to_owned());
        }
        let frame_rate = timings.len() as f64 * f64::from(VIDEO_TIMESCALE) / frame_ticks as f64;
        let gop_size = (frame_rate * SEGMENT_TARGET_SECONDS)
            .round()
            .clamp(1.0, f64::from(u32::MAX)) as u32;
        let mut encoder = H264Encoder::new(h264_encoder_config(
            width,
            height,
            gop_size,
            bitrate,
            frame_rate as f32,
        ))
        .map_err(|error| format!("create H.264 encoder: {error}"))?;
        let first_access_unit = encoder
            .try_encode(&YuvFrame::black(usize::from(width), usize::from(height)))
            .map_err(|error| format!("initialize H.264 encoder: {error}"))?;
        let (sps, pps) = extract_avc_parameter_sets(&first_access_unit)?;
        let codec = format!("avc1.{:02X}{:02X}{:02X}", sps[1], sps[2], sps[3]);
        Ok(Self {
            gop_size,
            bitrate,
            frame_rate: frame_rate as f32,
            sps,
            pps,
            codec,
        })
    }

    fn encoder(&self, width: u16, height: u16) -> Result<H264Encoder, String> {
        H264Encoder::new(h264_encoder_config(
            width,
            height,
            self.gop_size,
            self.bitrate,
            self.frame_rate,
        ))
        .map_err(|error| format!("create H.264 encoder: {error}"))
    }

    fn codec_config(&self, width: u16, height: u16) -> CodecConfig {
        CodecConfig::Avc {
            config: AVCConfigurationBox {
                config: AVCDecoderConfigurationRecord {
                    configuration_version: 1,
                    profile_indication: self.sps[1],
                    profile_compatibility: self.sps[2],
                    level_indication: self.sps[3],
                    length_size_minus_one: 3,
                    sps: vec![AvcSps(self.sps.clone())],
                    pps: vec![AvcPps(self.pps.clone())],
                    chroma_format: None,
                    bit_depth_luma_minus8: None,
                    bit_depth_chroma_minus8: None,
                    sps_ext: Vec::new(),
                },
            },
            width,
            height,
        }
    }
}

fn h264_encoder_config(
    width: u16,
    height: u16,
    gop_size: u32,
    bitrate: u32,
    frame_rate: f32,
) -> H264EncoderConfig {
    let mut config = H264EncoderConfig::new(usize::from(width), usize::from(height));
    config.gop_size = gop_size;
    config.bitrate = bitrate;
    config.framerate = frame_rate;
    config.level_idc = 41;
    config.preset = Preset::Fast;
    config.scenecut = 0;
    config.mbtree = false;
    config.bframes = 0;
    config.num_ref_frames = 1;
    config
}

fn transcode_segment_durations(
    timings: &[VideoFrameTiming],
    gop_size: u32,
) -> Result<Vec<f64>, String> {
    let Some(first) = timings.first() else {
        return Err("video track has no frames to transcode".to_owned());
    };
    let target_ticks = (f64::from(VIDEO_TIMESCALE) * SEGMENT_TARGET_SECONDS) as u64;
    let mut segment_start = first.pts;
    let mut pending_ticks = 0_u64;
    let mut durations = Vec::new();
    for (index, timing) in timings.iter().enumerate() {
        if index > 0 && index % gop_size as usize == 0 && pending_ticks >= target_ticks {
            durations.push(
                ticks_to_seconds(timing.pts.saturating_sub(segment_start), VIDEO_TIMESCALE)
                    .max(0.001),
            );
            segment_start = timing.pts;
            pending_ticks = 0;
        }
        pending_ticks = pending_ticks.saturating_add(u64::from(timing.duration));
    }
    let last = timings.last().expect("checked non-empty");
    let end = last.pts.saturating_add(i64::from(last.duration));
    durations.push(ticks_to_seconds(end.saturating_sub(segment_start), VIDEO_TIMESCALE).max(0.001));
    Ok(durations)
}

fn annex_b_nalus(data: &[u8]) -> Vec<&[u8]> {
    let mut starts = Vec::new();
    let mut offset = 0;
    while offset + 3 <= data.len() {
        let prefix = if data.get(offset..offset + 4) == Some(&[0, 0, 0, 1]) {
            4
        } else if data.get(offset..offset + 3) == Some(&[0, 0, 1]) {
            3
        } else {
            offset += 1;
            continue;
        };
        starts.push((offset, offset + prefix));
        offset += prefix;
    }
    starts
        .iter()
        .enumerate()
        .filter_map(|(index, (_, payload_start))| {
            let end = starts
                .get(index + 1)
                .map_or(data.len(), |(next_start, _)| *next_start);
            let mut nal = data.get(*payload_start..end)?;
            while nal.last() == Some(&0) {
                nal = &nal[..nal.len() - 1];
            }
            (!nal.is_empty()).then_some(nal)
        })
        .collect()
}

fn extract_avc_parameter_sets(data: &[u8]) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut sps = None;
    let mut pps = None;
    for nal in annex_b_nalus(data) {
        match nal.first().map(|byte| byte & 0x1f) {
            Some(7) => sps = Some(nal.to_vec()),
            Some(8) => pps = Some(nal.to_vec()),
            _ => {}
        }
    }
    let sps = sps.ok_or_else(|| "H.264 encoder did not emit an SPS".to_owned())?;
    let pps = pps.ok_or_else(|| "H.264 encoder did not emit a PPS".to_owned())?;
    if sps.len() < 4 {
        return Err("H.264 encoder emitted a truncated SPS".to_owned());
    }
    Ok((sps, pps))
}

fn annex_b_to_avcc_sample(data: &[u8]) -> Result<Option<(Vec<u8>, bool)>, String> {
    let mut sample = Vec::new();
    let mut has_video = false;
    let mut is_sync = false;
    for nal in annex_b_nalus(data) {
        let nal_type = nal[0] & 0x1f;
        if matches!(nal_type, 7 | 8 | 9 | 10 | 11 | 12) {
            continue;
        }
        if (1..=5).contains(&nal_type) {
            has_video = true;
            is_sync |= nal_type == 5;
        } else if nal_type != 6 {
            continue;
        }
        let size = u32::try_from(nal.len())
            .map_err(|_| "H.264 access unit NAL exceeds the MP4 size limit".to_owned())?;
        sample.extend_from_slice(&size.to_be_bytes());
        sample.extend_from_slice(nal);
    }
    if data.is_empty() {
        return Ok(None);
    }
    if !has_video {
        return Err("H.264 encoder emitted an access unit without video data".to_owned());
    }
    Ok(Some((sample, is_sync)))
}


fn hevc_decoder_from_config(config: &HEVCConfigurationBox) -> Result<HevcDecoder, String> {
    let mut decoder = HevcDecoder::new().with_display_order();
    for nal_type in [32, 33, 34] {
        let mut found = false;
        for array in &config.config.arrays {
            if array.nal_unit_type != nal_type {
                continue;
            }
            for nal in &array.nalus {
                let parsed = parse_nal(&nal.0)
                    .ok_or_else(|| "HEVC decoder configuration has an invalid NAL".to_owned())?;
                decoder.decode_nal(&parsed).map_err(media_error)?;
                found = true;
            }
        }
        if !found {
            return Err(format!("HEVC decoder configuration has no NAL type {nal_type}"));
        }
    }
    Ok(decoder)
}

fn yuv_frame_from_hevc(frame: rust_h265::Frame) -> Result<YuvFrame, String> {
    if frame.chroma_format_idc != 1 || !(8..=12).contains(&frame.bit_depth) {
        return Err("video conversion supports 4:2:0 HEVC up to 12 bits".to_owned());
    }
    let width = usize::try_from(frame.width).map_err(media_error)?;
    let height = usize::try_from(frame.height).map_err(media_error)?;
    if width % 2 != 0 || height % 2 != 0 {
        return Err("H.264 conversion requires even video dimensions".to_owned());
    }
    let convert_plane = |plane: &PixelData| match plane {
        PixelData::U8(samples) if frame.bit_depth == 8 => Ok(samples.clone()),
        PixelData::U16(samples) if frame.bit_depth > 8 => Ok(samples
            .iter()
            .map(|sample| (sample >> (frame.bit_depth - 8)).min(255) as u8)
            .collect()),
        _ => Err("HEVC decoder returned an unexpected pixel format".to_owned()),
    };
    Ok(YuvFrame {
        width,
        height,
        y: convert_plane(&frame.y)?,
        u: convert_plane(&frame.u)?,
        v: convert_plane(&frame.v)?,
    })
}

fn push_video_sample(
    segmenter: &mut Segmenter,
    sample: Sample,
    pts: i64,
    is_sync: bool,
    segment_start: &mut Option<i64>,
    cache: &Path,
    prefix: &str,
    segments: &mut Vec<SegmentInfo>,
    on_resource_ready: &mut impl FnMut(&str),
) -> Result<(), String> {
    if is_sync && segment_start.is_none() {
        *segment_start = Some(pts);
    }
    segmenter
        .push(TrackId::new(VIDEO_TRACK_ID), sample)
        .map_err(media_error)?;
    for segment in segmenter.take_ready() {
        let start = segment_start
            .ok_or_else(|| "segment began before a video sync sample".to_owned())?;
        let duration = ticks_to_seconds(pts.saturating_sub(start), VIDEO_TIMESCALE).max(0.001);
        let index = segments.len();
        let size = write_segment_named(cache, prefix, index, segment)?;
        on_resource_ready(&format!("{prefix}-{index}.m4s"));
        segments.push(SegmentInfo { duration, size });
        *segment_start = Some(pts);
    }
    Ok(())
}


fn push_audio_sample(
    segmenter: &mut Segmenter,
    sample: Sample,
    pts: i64,
    duration: u32,
    sample_rate: u32,
    segment_start: &mut Option<i64>,
    last_audio_end: &mut Option<i64>,
    cache: &Path,
    prefix: &str,
    segments: &mut Vec<SegmentInfo>,
    on_resource_ready: &mut impl FnMut(&str),
) -> Result<(), String> {
    segment_start.get_or_insert(pts);
    segmenter
        .push(TrackId::new(AUDIO_TRACK_ID), sample)
        .map_err(media_error)?;
    for segment in segmenter.take_ready() {
        let start = segment_start
            .ok_or_else(|| "audio segment began without a first sample".to_owned())?;
        let duration = ticks_to_seconds(pts.saturating_sub(start), sample_rate).max(0.001);
        let index = segments.len();
        let size = write_segment_named(cache, prefix, index, segment)?;
        on_resource_ready(&format!("{prefix}-{index}.m4s"));
        segments.push(SegmentInfo { duration, size });
        *segment_start = Some(pts);
    }
    *last_audio_end = Some(pts.saturating_add(i64::from(duration)));
    Ok(())
}

fn push_transcoded_video_frame(
    decoded: rust_h265::Frame,
    timing: VideoFrameTiming,
    frame_index: usize,
    config: &VideoTranscodeConfig,
    width: u16,
    height: u16,
    encoder: &mut H264Encoder,
    segmenter: &mut Segmenter,
    segment_start: &mut Option<i64>,
    last_video_end: &mut Option<i64>,
    cache: &Path,
    prefix: &str,
    segments: &mut Vec<SegmentInfo>,
    on_resource_ready: &mut impl FnMut(&str),
) -> Result<(), String> {
    if decoded.width != u32::from(width) || decoded.height != u32::from(height) {
        return Err("decoded HEVC dimensions do not match the track metadata".to_owned());
    }
    let yuv = yuv_frame_from_hevc(decoded)?;
    let encoded = encoder
        .try_encode(&yuv)
        .map_err(|error| format!("encode H.264 frame: {error}"))?;
    let (sample_data, is_sync) = annex_b_to_avcc_sample(&encoded)?
        .ok_or_else(|| "H.264 encoder buffered a frame unexpectedly".to_owned())?;
    if is_sync != (frame_index % config.gop_size as usize == 0) {
        return Err("H.264 encoder GOP cadence differed from the DASH timeline".to_owned());
    }
    let sample = Sample::new(
        Bytes::from(sample_data),
        Some(timing.pts),
        Some(timing.pts),
        Some(timing.duration),
        is_sync,
    );
    push_video_sample(
        segmenter,
        sample,
        timing.pts,
        is_sync,
        segment_start,
        cache,
        prefix,
        segments,
        on_resource_ready,
    )?;
    *last_video_end = Some(timing.pts.saturating_add(i64::from(timing.duration)));
    Ok(())
}

fn remux_matroska(
    path: &Path,
    cache: &Path,
    transcode_audio: bool,
    video_transcode: Option<&VideoTranscodeConfig>,
    video_codec_id: &str,
    video_timings: &[VideoFrameTiming],
    video_segment_start_pts: &[i64],
    video_segment_start_ticks: &[u64],
    audio_timings: &[AudioFrameTiming],
    audio_sample_rate_hint: Option<u32>,
    separate_dash_tracks: bool,
    matroska_hls: bool,
    segment_templates: &[SegmentInfo],
    video_start_segment: usize,
    audio_start_segment: usize,
    video_start_pts: i64,
    video_decode_start_pts: i64,
    audio_start_pts: i64,
    audio_decode_start_pts: i64,
    audio_source_seek_pts: i64,
    source_seek_timestamp: u64,
    should_seek_source: bool,
    wait_for_initial_muxed_segment: &mut impl FnMut() -> Result<Option<usize>, String>,
    wait_for_capacity: &mut impl FnMut(usize, usize, usize, usize) -> Result<(), String>,
    on_resource_ready: &mut impl FnMut(&str),
) -> Result<BuiltSegments, String> {
    let mut probe =
        MatroskaFile::open(File::open(path).map_err(media_error)?).map_err(media_error)?;
    let timestamp_scale = probe.info().timestamp_scale().get();
    let video_track = probe
        .tracks()
        .iter()
        .find(|track| track.track_type() == TrackType::Video && track.codec_id() == video_codec_id)
        .ok_or_else(|| format!("source video track {video_codec_id} is unavailable"))?;
    let audio_track = probe.tracks().iter().find(|track| {
        track.track_type() == TrackType::Audio
            && (track.codec_id() == "A_EAC3"
                || (matroska_hls && !transcode_audio && track.codec_id() == "A_OPUS"))
    });
    let has_audio = probe
        .tracks()
        .iter()
        .any(|track| track.track_type() == TrackType::Audio);
    if has_audio && audio_track.is_none() {
        let codecs = probe
            .tracks()
            .iter()
            .filter(|track| track.track_type() == TrackType::Audio)
            .map(|track| track.codec_id())
            .collect::<Vec<_>>();
        return Err(format!(
            "source audio codec is not supported for remuxing; audio CodecIDs: {codecs:?}"
        ));
    }
    let source_audio = audio_track
        .map(|track| -> Result<SourceAudioTrack, String> {
            let audio_settings = track
                .audio()
                .ok_or_else(|| "audio track has no audio settings".to_owned())?;
            let source_sample_rate = audio_settings.sampling_frequency().round() as u32;
            let source_channels =
                u16::try_from(audio_settings.channels().get()).map_err(media_error)?;
            let codec_id = track.codec_id().to_owned();
            let codec_private = track.codec_private().unwrap_or_default().to_vec();
            let (sample_rate, packet_timescale, channels, codec_delay, seek_pre_roll) =
                if codec_id == "A_OPUS" {
                    let (sample_rate, channels, codec_delay) =
                        opus_track_config(&codec_private, source_sample_rate, source_channels)?;
                    if track
                        .codec_delay()
                        .is_some_and(|delay| delay != codec_delay)
                    {
                        return Err("Opus CodecDelay does not match OpusHead pre-skip".to_owned());
                    }
                    (
                        sample_rate,
                        48_000,
                        channels,
                        Some(codec_delay),
                        Some(80_000_000),
                    )
                } else {
                    (
                        source_sample_rate,
                        source_sample_rate,
                        source_channels,
                        track.codec_delay(),
                        None,
                    )
                };
            Ok(SourceAudioTrack {
                number: track.track_number().get(),
                codec_id,
                codec_private,
                sample_rate,
                packet_timescale,
                channels,
                codec_delay,
                seek_pre_roll,
            })
        })
        .transpose()?;

    let video_number = video_track.track_number().get();
    let video_private = video_track
        .codec_private()
        .ok_or_else(|| "video track has no decoder configuration".to_owned())?
        .to_vec();
    let nal_length_size = video_nal_length_size(video_codec_id, &video_private)?;
    let video_settings = video_track
        .video()
        .ok_or_else(|| "video track has no video settings".to_owned())?;
    let width = u16::try_from(video_settings.pixel_width().get())
        .map_err(|_| "video width is outside the supported range".to_owned())?;
    let height = u16::try_from(video_settings.pixel_height().get())
        .map_err(|_| "video height is outside the supported range".to_owned())?;
    let video_default_duration = video_track
        .default_duration()
        .map(|duration| duration.get());
    let video_codec_delay = video_track.codec_delay().unwrap_or(0);

    let audio_number = source_audio.as_ref().map(|audio| audio.number);
    let (audio_spec, mut audio_transcoder) = if let Some(audio) = source_audio.as_ref() {
        let sample_rate = audio.packet_timescale;
        if transcode_audio {
            let asc = rusty_aac::encode::audio_specific_config_bytes(sample_rate, 2);
            let config = transmux::rtp_sdp::aac_config_from_asc_bytes(asc).map_err(media_error)?;
            let spec = TrackSpec::new(TrackId::new(AUDIO_TRACK_ID), sample_rate, config);
            let initial_pts = if separate_dash_tracks && should_seek_source {
                Some(audio_decode_start_pts)
            } else {
                None
            };
            let transcoder =
                Eac3AacTranscoder::new(sample_rate, initial_pts, !separate_dash_tracks)?;
            (Some(spec), Some(transcoder))
        } else if matroska_hls {
            (None, None)
        } else {
            let mut first_audio = None;
            let mut frame = Frame::default();
            while probe.next_frame(&mut frame).map_err(media_error)? {
                if frame.track == audio.number {
                    first_audio = Some(frame.data.clone());
                    break;
                }
            }
            let first_audio =
                first_audio.ok_or_else(|| "E-AC-3 track has no samples".to_owned())?;
            let syncframes = Ec3SyncframeInfo::from_es_first_au(&first_audio);
            let config = Ec3SpecificBox::from_syncframes(&syncframes).map_err(media_error)?;
            let channels = u16::from(config.channel_count()).max(audio.channels);
            let config = CodecConfig::Eac3 {
                config,
                channel_count: channels,
                sample_rate,
                sample_size: 16,
            };
            let spec = TrackSpec::new(TrackId::new(AUDIO_TRACK_ID), sample_rate, config);
            (Some(spec), None)
        }
    } else if transcode_audio {
        return Err("audio transcoding was requested for a source with no E-AC-3 track".to_owned());
    } else {
        (None, None)
    };

    let hevc_config = if video_codec_id == "V_MPEGH/ISO/HEVC" {
        Some(HEVCConfigurationBox::parse_body(&video_private).map_err(media_error)?)
    } else {
        None
    };
    let mut video_decoder = video_transcode
        .map(|_| {
            hevc_decoder_from_config(
                hevc_config
                    .as_ref()
                    .ok_or_else(|| "video transcoding requires an HEVC source".to_owned())?,
            )
        })
        .transpose()?;
    let mut video_encoder = video_transcode
        .map(|config| config.encoder(width, height))
        .transpose()?;
    let video_config = if let Some(config) = video_transcode {
        config.codec_config(width, height)
    } else {
        match video_codec_id {
            "V_MPEGH/ISO/HEVC" => CodecConfig::Hevc {
                config: hevc_config.ok_or_else(|| "HEVC configuration is missing".to_owned())?,
                width,
                height,
            },
            "V_MPEG4/ISO/AVC" => CodecConfig::Avc {
                config: AVCConfigurationBox::parse_body(&video_private).map_err(media_error)?,
                width,
                height,
            },
            _ => return Err(format!("video codec {video_codec_id} is not supported")),
        }
    };
    let video_spec = TrackSpec::new(TrackId::new(VIDEO_TRACK_ID), VIDEO_TIMESCALE, video_config);
    let audio_sample_rate = audio_spec.as_ref().map(|spec| spec.timescale).or_else(|| {
        if matroska_hls {
            source_audio.as_ref().map(|audio| audio.packet_timescale)
        } else {
            None
        }
    });
    if audio_sample_rate_hint.is_some_and(|expected| Some(expected) != audio_sample_rate) {
        return Err("audio sample rate changed between DASH preparation and remux".to_owned());
    }
    let mut segmenter = None;
    let mut video_segmenter = None;
    let mut audio_segmenter = None;
    if matroska_hls {
        if separate_dash_tracks || video_transcode.is_some() {
            return Err("Matroska HLS requires copied video".to_owned());
        }
        let sample_rate =
            audio_sample_rate.ok_or_else(|| "Matroska HLS requires an audio track".to_owned())?;
        let asc = if transcode_audio {
            Some(rusty_aac::encode::audio_specific_config_bytes(
                sample_rate,
                2,
            ))
        } else {
            None
        };
        let audio = if let Some(asc) = asc.as_ref() {
            Some(mkv_hls::AudioTrack {
                codec_id: "A_AAC",
                codec_private: asc,
                sample_rate,
                channels: 2,
                codec_delay: None,
                seek_pre_roll: None,
            })
        } else {
            let source = source_audio
                .as_ref()
                .ok_or_else(|| "Matroska HLS requires an audio track".to_owned())?;
            Some(mkv_hls::AudioTrack {
                codec_id: &source.codec_id,
                codec_private: &source.codec_private,
                sample_rate: source.sample_rate,
                channels: source.channels,
                codec_delay: source.codec_delay,
                seek_pre_roll: source.seek_pre_roll,
            })
        };
        let header = mkv_hls::initialization_segment(
            mkv_hls::VideoTrack {
                codec_id: video_codec_id,
                codec_private: &video_private,
                width,
                height,
            },
            audio,
        )?;
        fs::write(cache.join("init.mkv"), header).map_err(media_error)?;
        on_resource_ready("init.mkv");
    } else if separate_dash_tracks {
        let video = Segmenter::new(vec![video_spec], 1000, SEGMENT_TARGET_SECONDS)
            .map_err(media_error)?;
        fs::write(cache.join("init-video.mp4"), video.init_segment().map_err(media_error)?)
            .map_err(media_error)?;
        on_resource_ready("init-video.mp4");
        video_segmenter = Some(video);
        if let Some(audio_spec) = audio_spec {
            let audio = Segmenter::new(vec![audio_spec], 1000, SEGMENT_TARGET_SECONDS)
                .map_err(media_error)?;
            fs::write(cache.join("init-audio.mp4"), audio.init_segment().map_err(media_error)?)
                .map_err(media_error)?;
            on_resource_ready("init-audio.mp4");
            audio_segmenter = Some(audio);
        }
    } else {
        let mut specs = vec![video_spec];
        if let Some(audio_spec) = audio_spec {
            specs.push(audio_spec);
        }
        let muxed = Segmenter::new(specs, 1000, SEGMENT_TARGET_SECONDS).map_err(media_error)?;
        fs::write(cache.join("init.mp4"), muxed.init_segment().map_err(media_error)?)
            .map_err(media_error)?;
        on_resource_ready("init.mp4");
        segmenter = Some(muxed);
    }

    let mut video_start_segment = video_start_segment;
    let mut video_start_pts = video_start_pts;
    let mut video_decode_start_pts = video_decode_start_pts;
    let mut audio_source_seek_pts = audio_source_seek_pts;
    let mut source_seek_timestamp = source_seek_timestamp;
    let mut should_seek_source = should_seek_source;
    if let Some(requested_segment) = wait_for_initial_muxed_segment()? {
        if !separate_dash_tracks && video_transcode.is_none() {
            let (requested_segment, requested_pts, requested_ticks) = hls_segment_start(
                requested_segment,
                video_start_segment,
                video_segment_start_pts,
                video_segment_start_ticks,
            )?;
            if requested_segment > video_start_segment {
                video_start_segment = requested_segment;
                video_start_pts = requested_pts;
                video_decode_start_pts = requested_pts;
                source_seek_timestamp = requested_ticks;
                should_seek_source = true;
            }
        }
    }
    if !separate_dash_tracks && should_seek_source {
        if let Some(sample_rate) = audio_sample_rate {
            audio_source_seek_pts = i64::try_from(
                i128::from(video_start_pts) * i128::from(sample_rate)
                    / i128::from(VIDEO_TIMESCALE),
            )
            .map_err(|_| "HLS audio seek timestamp is outside the supported range".to_owned())?;
        }
    }

    let mut source =
        MatroskaFile::open(File::open(path).map_err(media_error)?).map_err(media_error)?;
    let mut frame = Frame::default();
    let mut video_dts = None::<i64>;
    let mut video_first_dts = None::<i64>;
    let mut video_segment_start = None::<i64>;
    let mut last_video_end = None::<i64>;
    let mut segments = std::iter::repeat_with(|| SegmentInfo {
        duration: 0.0,
        size: 0,
    })
    .take(video_start_segment)
    .collect::<Vec<_>>();
    let mut audio_segments = std::iter::repeat_with(|| SegmentInfo {
        duration: 0.0,
        size: 0,
    })
    .take(audio_start_segment)
    .collect::<Vec<_>>();
    let mut matroska_hls_builder = if matroska_hls {
        Some(mkv_hls::SegmentBuilder::new(
            video_segment_start_pts,
            video_start_segment,
            audio_number.is_some(),
        ))
    } else {
        None
    };
    let mut audio_segment_start = None::<i64>;
    let mut last_audio_end = None::<i64>;
    let audio_index_start_pts = if transcode_audio {
        audio_decode_start_pts
    } else {
        audio_start_pts
    };
    let mut audio_frame_index = if should_seek_source {
        audio_timings.partition_point(|timing| timing.pts < audio_index_start_pts)
    } else {
        0
    };
    let video_index_start_pts = if video_transcode.is_some() {
        video_decode_start_pts
    } else {
        video_start_pts
    };
    let mut video_frame_index = if should_seek_source {
        video_timings.partition_point(|timing| timing.pts < video_index_start_pts)
    } else {
        0
    };
    let mut video_processing_time = Duration::ZERO;
    let mut video_processing_frames = 0_usize;
    let mut audio_processing_time = Duration::ZERO;
    let mut audio_processing_frames = 0_usize;
    let mut last_processing_report = Instant::now();
    let mut audio_codec_delay = 0_u64;
    if let Some(number) = audio_number {
        audio_codec_delay = source
            .tracks()
            .iter()
            .find(|track| track.track_number().get() == number)
            .and_then(|track| track.codec_delay())
            .unwrap_or(0);
    }
    let perform_matroska_seek = should_seek_matroska_source(
        should_seek_source,
        separate_dash_tracks,
        video_transcode.is_some(),
    );
    if perform_matroska_seek {
        source
            .seek(source_seek_timestamp.saturating_sub(1))
            .map_err(media_error)?;
    }

    loop {
        let video_work_ahead = segments.len()
            + matroska_hls_builder
                .as_ref()
                .map_or(0, mkv_hls::SegmentBuilder::pending_count);
        wait_for_capacity(
            video_work_ahead,
            audio_segments.len(),
            video_start_segment,
            audio_start_segment,
        )?;
        if !source.next_frame(&mut frame).map_err(media_error)? {
            break;
        }
        let presentation_ns = frame_timestamp_ns(frame.timestamp, timestamp_scale)?;
        if should_seek_source && video_transcode.is_none() && frame.track == video_number {
            let pts = ns_to_ticks(
                presentation_ns.saturating_sub(video_codec_delay),
                VIDEO_TIMESCALE,
            )?;
            if pts < video_start_pts {
                continue;
            }
        }
        if frame.track == video_number {
            let processing_started = Instant::now();
            if let Some(config) = video_transcode {
                let decoder = video_decoder
                    .as_mut()
                    .ok_or_else(|| "HEVC decoder is not available".to_owned())?;
                let nals = parse_hvcc(
                    &frame.data,
                    u8::try_from(nal_length_size).map_err(media_error)?,
                );
                if nals.is_empty() {
                    return Err("invalid length-prefixed HEVC access unit".to_owned());
                }
                for nal in nals {
                    decoder.decode_nal(&nal).map_err(media_error)?;
                    while let Some(decoded) = decoder.next_output() {
                        let timing = *video_timings.get(video_frame_index).ok_or_else(|| {
                            "HEVC decoder produced more frames than the source timeline".to_owned()
                        })?;
                        if should_seek_source && timing.pts < video_start_pts {
                            video_frame_index += 1;
                            continue;
                        }
                        let target_segmenter = if separate_dash_tracks {
                            video_segmenter
                                .as_mut()
                                .ok_or_else(|| "video segmenter is not available".to_owned())?
                        } else {
                            segmenter
                                .as_mut()
                                .ok_or_else(|| "muxed segmenter is not available".to_owned())?
                        };
                        let prefix = if separate_dash_tracks { "video-segment" } else { "segment" };
                        push_transcoded_video_frame(
                            decoded,
                            timing,
                            video_frame_index,
                            config,
                            width,
                            height,
                            video_encoder
                                .as_mut()
                                .ok_or_else(|| "H.264 encoder is not available".to_owned())?,
                            target_segmenter,
                            &mut video_segment_start,
                            &mut last_video_end,
                            cache,
                            prefix,
                            &mut segments,
                            on_resource_ready,
                        )?;
                        video_frame_index += 1;
                    }
                }
            } else {
                let duration_ns = frame
                    .duration
                    .map(|duration| duration.saturating_mul(timestamp_scale))
                    .or(video_default_duration)
                    .ok_or_else(|| "video track has no frame duration metadata".to_owned())?;
                let pts = ns_to_ticks(
                    presentation_ns.saturating_sub(video_codec_delay),
                    VIDEO_TIMESCALE,
                )?;
                let duration = ns_to_duration(duration_ns, VIDEO_TIMESCALE)?;
                let dts = *video_dts.get_or_insert(pts);
                let first_dts = *video_first_dts.get_or_insert(dts);
                let segment_start = video_segment_start.unwrap_or(first_dts);
                if dts.saturating_sub(segment_start) > MAX_PENDING_VIDEO_TICKS {
                    return Err("video GOP exceeds the bounded segment window".to_owned());
                }
                let is_sync = frame.is_keyframe.unwrap_or_else(|| {
                    is_video_sync_sample(video_codec_id, &frame.data, nal_length_size)
                });
                let sample = Sample::new(
                    Bytes::from(frame.data.clone()),
                    Some(dts),
                    Some(pts),
                    Some(duration),
                    is_sync,
                );
                if let Some(builder) = matroska_hls_builder.as_mut() {
                    if is_sync {
                        video_segment_start = Some(pts);
                    }
                    builder.push_video(
                        pts,
                        dts,
                        Bytes::from(frame.data.clone()),
                        is_sync,
                    )?;
                    builder.flush_ready(
                        segment_templates,
                        &mut segments,
                        cache,
                        on_resource_ready,
                    )?;
                } else {
                    let target_segmenter = if separate_dash_tracks {
                        video_segmenter
                            .as_mut()
                            .ok_or_else(|| "video segmenter is not available".to_owned())?
                    } else {
                        segmenter
                            .as_mut()
                            .ok_or_else(|| "muxed segmenter is not available".to_owned())?
                    };
                    let prefix = if separate_dash_tracks { "video-segment" } else { "segment" };
                    push_video_sample(
                        target_segmenter,
                        sample,
                        pts,
                        is_sync,
                        &mut video_segment_start,
                        cache,
                        prefix,
                        &mut segments,
                        on_resource_ready,
                    )?;
                }
                video_dts = Some(dts.saturating_add(i64::from(duration)));
                last_video_end = Some(pts.saturating_add(i64::from(duration)));
            }
            video_processing_time += processing_started.elapsed();
            video_processing_frames += 1;
        } else if Some(frame.track) == audio_number {
            let processing_started = Instant::now();
            let Some(sample_rate) = audio_sample_rate else {
                continue;
            };
            let audio_presentation_ns = if matroska_hls && !transcode_audio {
                presentation_ns
            } else {
                presentation_ns.saturating_sub(audio_codec_delay)
            };
            let pts = ns_to_ticks(audio_presentation_ns, sample_rate)?;
            if should_seek_source && pts < audio_source_seek_pts {
                continue;
            }
            if matroska_hls && !transcode_audio {
                let builder = matroska_hls_builder
                    .as_mut()
                    .ok_or_else(|| "Matroska HLS segment builder is not available".to_owned())?;
                builder.push_audio(pts, sample_rate, Bytes::from(frame.data.clone()))?;
                builder.flush_ready(segment_templates, &mut segments, cache, on_resource_ready)?;
            } else if let Some(transcoder) = audio_transcoder.as_mut() {
                for sample in transcoder.push(&frame.data, pts)? {
                    if let Some(builder) = matroska_hls_builder.as_mut() {
                        let sample_pts = sample
                            .pts
                            .ok_or_else(|| "AAC transcoder returned a sample without PTS".to_owned())?;
                        builder.push_audio(sample_pts, sample_rate, sample.data.clone())?;
                        builder.flush_ready(
                            segment_templates,
                            &mut segments,
                            cache,
                            on_resource_ready,
                        )?;
                    } else if separate_dash_tracks {
                        let sample_pts = sample
                            .pts
                            .ok_or_else(|| "AAC transcoder returned a sample without PTS".to_owned())?;
                        let duration = sample.duration.ok_or_else(|| {
                            "AAC transcoder returned a sample without duration".to_owned()
                        })?;
                        check_audio_timing(audio_timings, audio_frame_index, sample_pts, duration)?;
                        audio_frame_index += 1;
                        if should_seek_source && sample_pts < audio_start_pts {
                            continue;
                        }
                        push_audio_sample(
                            audio_segmenter
                                .as_mut()
                                .ok_or_else(|| "audio segmenter is not available".to_owned())?,
                            sample,
                            sample_pts,
                            duration,
                            sample_rate,
                            &mut audio_segment_start,
                            &mut last_audio_end,
                            cache,
                            "audio-segment",
                            &mut audio_segments,
                            on_resource_ready,
                        )?;
                    } else {
                        segmenter
                            .as_mut()
                            .ok_or_else(|| "muxed segmenter is not available".to_owned())?
                            .push(TrackId::new(AUDIO_TRACK_ID), sample)
                            .map_err(media_error)?;
                    }
                }
            } else {
                let syncframes = Ec3SyncframeInfo::from_es_first_au(&frame.data);
                let Some(syncframe) = syncframes.first() else {
                    return Err("invalid E-AC-3 sample".to_owned());
                };
                if syncframe.sample_rate != sample_rate {
                    return Err("E-AC-3 sample rate does not match its track metadata".to_owned());
                }
                let duration = u32::from(syncframe.numblks) * 256;
                let sample = Sample::new(
                    Bytes::from(frame.data.clone()),
                    Some(pts),
                    Some(pts),
                    Some(duration),
                    true,
                );
                if separate_dash_tracks {
                    check_audio_timing(audio_timings, audio_frame_index, pts, duration)?;
                    push_audio_sample(
                        audio_segmenter
                            .as_mut()
                            .ok_or_else(|| "audio segmenter is not available".to_owned())?,
                        sample,
                        pts,
                        duration,
                        sample_rate,
                        &mut audio_segment_start,
                        &mut last_audio_end,
                        cache,
                        "audio-segment",
                        &mut audio_segments,
                        on_resource_ready,
                    )?;
                    audio_frame_index += 1;
                } else {
                    segmenter
                        .as_mut()
                        .ok_or_else(|| "muxed segmenter is not available".to_owned())?
                        .push(TrackId::new(AUDIO_TRACK_ID), sample)
                        .map_err(media_error)?;
                }
            }
            audio_processing_time += processing_started.elapsed();
            audio_processing_frames += 1;
        }
        if last_processing_report.elapsed() >= Duration::from_secs(5) {
            tracing::info!(
                video_frames = video_processing_frames,
                video_processing_ms = video_processing_time.as_secs_f64() * 1000.0,
                audio_frames = audio_processing_frames,
                audio_processing_ms = audio_processing_time.as_secs_f64() * 1000.0,
                "Plex remux processing timing"
            );
            video_processing_time = Duration::ZERO;
            video_processing_frames = 0;
            audio_processing_time = Duration::ZERO;
            audio_processing_frames = 0;
            last_processing_report = Instant::now();
        }
    }
    if let Some(config) = video_transcode {
        let decoder = video_decoder
            .as_mut()
            .ok_or_else(|| "HEVC decoder is not available".to_owned())?;
        let encoder = video_encoder
            .as_mut()
            .ok_or_else(|| "H.264 encoder is not available".to_owned())?;
        while let Some(decoded) = decoder.flush() {
            let timing = *video_timings.get(video_frame_index).ok_or_else(|| {
                "HEVC decoder produced more frames than the source timeline".to_owned()
            })?;
            let target_segmenter = if separate_dash_tracks {
                video_segmenter
                    .as_mut()
                    .ok_or_else(|| "video segmenter is not available".to_owned())?
            } else {
                segmenter
                    .as_mut()
                    .ok_or_else(|| "muxed segmenter is not available".to_owned())?
            };
            let prefix = if separate_dash_tracks { "video-segment" } else { "segment" };
            push_transcoded_video_frame(
                decoded,
                timing,
                video_frame_index,
                config,
                width,
                height,
                encoder,
                target_segmenter,
                &mut video_segment_start,
                &mut last_video_end,
                cache,
                prefix,
                &mut segments,
                on_resource_ready,
            )?;
            video_frame_index += 1;
        }
        if video_frame_index != video_timings.len() {
            return Err("HEVC decoder output did not match the source frame timeline".to_owned());
        }
        if !encoder.flush().is_empty() {
            return Err("H.264 encoder buffered unexpected tail frames".to_owned());
        }
    }

    if let Some(transcoder) = audio_transcoder.as_mut() {
        for sample in transcoder.finish()? {
            if let Some(builder) = matroska_hls_builder.as_mut() {
                let pts = sample
                    .pts
                    .ok_or_else(|| "AAC transcoder returned a sample without PTS".to_owned())?;
                builder.push_audio(
                    pts,
                    audio_sample_rate.ok_or_else(|| "audio sample rate is missing".to_owned())?,
                    sample.data.clone(),
                )?;
                builder.flush_ready(segment_templates, &mut segments, cache, on_resource_ready)?;
            } else if separate_dash_tracks {
                let pts = sample
                    .pts
                    .ok_or_else(|| "AAC transcoder returned a sample without PTS".to_owned())?;
                let duration = sample.duration.ok_or_else(|| {
                    "AAC transcoder returned a sample without duration".to_owned()
                })?;
                check_audio_timing(audio_timings, audio_frame_index, pts, duration)?;
                audio_frame_index += 1;
                if should_seek_source && pts < audio_start_pts {
                    continue;
                }
                push_audio_sample(
                    audio_segmenter
                        .as_mut()
                        .ok_or_else(|| "audio segmenter is not available".to_owned())?,
                    sample,
                    pts,
                    duration,
                    audio_sample_rate.ok_or_else(|| "audio sample rate is missing".to_owned())?,
                    &mut audio_segment_start,
                    &mut last_audio_end,
                    cache,
                    "audio-segment",
                    &mut audio_segments,
                    on_resource_ready,
                )?;
            } else {
                segmenter
                    .as_mut()
                    .ok_or_else(|| "muxed segmenter is not available".to_owned())?
                    .push(TrackId::new(AUDIO_TRACK_ID), sample)
                    .map_err(media_error)?;
            }
        }
    }
    if separate_dash_tracks && audio_frame_index != audio_timings.len() {
        return Err("audio output did not match the prepared DASH timeline".to_owned());
    }

    if separate_dash_tracks {
        let mut video = video_segmenter
            .take()
            .ok_or_else(|| "video segmenter is not available".to_owned())?;
        video.flush().map_err(media_error)?;
        for segment in video.take_ready() {
            let start = video_segment_start
                .ok_or_else(|| "final video segment has no sync sample".to_owned())?;
            let end = last_video_end.ok_or_else(|| "video track has no frames".to_owned())?;
            let duration = ticks_to_seconds(end.saturating_sub(start), VIDEO_TIMESCALE).max(0.001);
            let index = segments.len();
            let size = write_segment_named(cache, "video-segment", index, segment)?;
            on_resource_ready(&format!("video-segment-{index}.m4s"));
            segments.push(SegmentInfo { duration, size });
        }
        if let Some(mut audio) = audio_segmenter.take() {
            audio.flush().map_err(media_error)?;
            for segment in audio.take_ready() {
                let start = audio_segment_start
                    .ok_or_else(|| "final audio segment has no first sample".to_owned())?;
                let end = last_audio_end.ok_or_else(|| "audio track has no samples".to_owned())?;
                let sample_rate = audio_sample_rate
                    .ok_or_else(|| "audio sample rate is missing".to_owned())?;
                let duration = ticks_to_seconds(end.saturating_sub(start), sample_rate).max(0.001);
                let index = audio_segments.len();
                let size = write_segment_named(cache, "audio-segment", index, segment)?;
                on_resource_ready(&format!("audio-segment-{index}.m4s"));
                audio_segments.push(SegmentInfo { duration, size });
            }
        }
    } else if let Some(builder) = matroska_hls_builder.as_mut() {
        builder.finish(segment_templates, &mut segments, cache, on_resource_ready)?;
    } else {
        let mut muxed = segmenter
            .take()
            .ok_or_else(|| "muxed segmenter is not available".to_owned())?;
        muxed.flush().map_err(media_error)?;
        for segment in muxed.take_ready() {
            let start = video_segment_start
                .ok_or_else(|| "final segment has no video sync sample".to_owned())?;
            let end = last_video_end.ok_or_else(|| "video track has no frames".to_owned())?;
            let duration = ticks_to_seconds(end.saturating_sub(start), VIDEO_TIMESCALE).max(0.001);
            let index = segments.len();
            let size = write_segment(cache, index, segment)?;
            on_resource_ready(&format!("segment-{index}.m4s"));
            segments.push(SegmentInfo { duration, size });
        }
    }
    Ok(BuiltSegments {
        video: segments,
        audio: audio_segments,
        video_start_segment,
    })
}

fn frame_timestamp_ns(timestamp: u64, timestamp_scale: u64) -> Result<u64, String> {
    timestamp
        .checked_mul(timestamp_scale)
        .ok_or_else(|| "media timestamp overflow".to_owned())
}

fn ns_to_ticks(nanoseconds: u64, timescale: u32) -> Result<i64, String> {
    let ticks = u128::from(nanoseconds)
        .checked_mul(u128::from(timescale))
        .ok_or_else(|| "media timestamp overflow".to_owned())?
        / 1_000_000_000;
    i64::try_from(ticks).map_err(|_| "media timestamp exceeds the CMAF range".to_owned())
}

fn ns_to_duration(nanoseconds: u64, timescale: u32) -> Result<u32, String> {
    let ticks = u128::from(nanoseconds)
        .checked_mul(u128::from(timescale))
        .ok_or_else(|| "media duration overflow".to_owned())?
        / 1_000_000_000;
    u32::try_from(ticks).map_err(|_| "media duration exceeds the CMAF range".to_owned())
}

fn ticks_to_seconds(ticks: i64, timescale: u32) -> f64 {
    ticks as f64 / f64::from(timescale)
}

fn video_nal_length_size(codec_id: &str, private: &[u8]) -> Result<usize, String> {
    let offset = match codec_id {
        "V_MPEGH/ISO/HEVC" => 21,
        "V_MPEG4/ISO/AVC" => 4,
        _ => return Err(format!("video codec {codec_id} is not supported")),
    };
    let length_size = private
        .get(offset)
        .ok_or_else(|| "video decoder configuration is too short".to_owned())?;
    Ok(usize::from(*length_size & 0x03) + 1)
}

fn is_video_sync_sample(codec_id: &str, data: &[u8], length_size: usize) -> bool {
    match codec_id {
        "V_MPEGH/ISO/HEVC" => is_hevc_sync_sample(data, length_size),
        "V_MPEG4/ISO/AVC" => is_avc_sync_sample(data, length_size),
        _ => false,
    }
}

fn is_avc_sync_sample(data: &[u8], length_size: usize) -> bool {
    if !(1..=4).contains(&length_size) {
        return false;
    }
    let mut offset: usize = 0;
    while let Some(nal_start) = offset.checked_add(length_size) {
        if nal_start >= data.len() {
            return false;
        }
        let Some(length_bytes) = data.get(offset..nal_start) else {
            return false;
        };
        let length = length_bytes.iter().fold(0_usize, |value, byte| {
            value.saturating_mul(256).saturating_add(usize::from(*byte))
        });
        let Some(nal_end) = nal_start.checked_add(length) else {
            return false;
        };
        if length == 0 || nal_end > data.len() {
            return false;
        }
        if data[nal_start] & 0x1f == 5 {
            return true;
        }
        offset = nal_end;
    }
    false
}

fn is_hevc_sync_sample(data: &[u8], length_size: usize) -> bool {
    if !(1..=4).contains(&length_size) {
        return false;
    }
    let mut offset = 0;
    while offset + length_size + 2 <= data.len() {
        let Some(length_bytes) = data.get(offset..offset + length_size) else {
            return false;
        };
        let length = length_bytes.iter().fold(0_usize, |value, byte| {
            value.saturating_mul(256).saturating_add(usize::from(*byte))
        });
        if length < 2 || offset + length_size + length > data.len() {
            return false;
        }
        let nal_type = (data[offset + length_size] >> 1) & 0x3f;
        if (16..=21).contains(&nal_type) {
            return true;
        }
        offset += length_size + length;
    }
    false
}

fn write_segment(cache: &Path, index: usize, bytes: Vec<u8>) -> Result<u64, String> {
    write_segment_named(cache, "segment", index, bytes)
}

fn write_segment_named(
    cache: &Path,
    prefix: &str,
    index: usize,
    bytes: Vec<u8>,
) -> Result<u64, String> {
    let size = u64::try_from(bytes.len()).map_err(media_error)?;
    fs::write(cache.join(format!("{prefix}-{index}.m4s")), bytes).map_err(media_error)?;
    Ok(size)
}

fn media_error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

struct Eac3AacTranscoder {
    decoder: Box<dyn Decoder>,
    encoder: Option<AacStreamEncoder>,
    bounded_encoder: bool,
    pcm_preroll: Vec<i16>,
    pending_pcm: Vec<i16>,
    pending_pcm_start: usize,
    sample_rate: u32,
    first_pts: Option<i64>,
    encoded_frames: u64,
    audio_decode_pcm_time: Duration,
    aac_encode_time: Duration,
    audio_packets_since_report: usize,
}

impl Eac3AacTranscoder {
    fn new(
        sample_rate: u32,
        initial_pts: Option<i64>,
        bounded_encoder: bool,
    ) -> Result<Self, String> {
        let mut params = CodecParameters::audio(CodecId::new("eac3"));
        params.sample_rate = Some(sample_rate);
        params.channels = Some(2);
        params.sample_format = Some(SampleFormat::F32);
        let decoder = make_eac3_decoder(&params).map_err(media_error)?;
        let encoder = (!bounded_encoder)
            .then(|| {
                AacStreamEncoder::new(AacEncoderConfig {
                    sample_rate,
                    channels: 2,
                    bitrate: 128_000,
                })
            })
            .transpose()
            .map_err(media_error)?;
        Ok(Self {
            decoder,
            encoder,
            bounded_encoder,
            pcm_preroll: Vec::with_capacity(AAC_FRAME_SAMPLES * AAC_CHANNELS),
            pending_pcm: Vec::with_capacity(if bounded_encoder {
                AAC_BOUNDED_CHUNK_SAMPLES
            } else {
                AAC_FRAME_SAMPLES * AAC_CHANNELS
            }),
            pending_pcm_start: 0,
            sample_rate,
            first_pts: initial_pts,
            encoded_frames: 0,
            audio_decode_pcm_time: Duration::ZERO,
            aac_encode_time: Duration::ZERO,
            audio_packets_since_report: 0,
        })
    }

    fn push(&mut self, data: &[u8], pts: i64) -> Result<Vec<Sample>, String> {
        const HOP_SAMPLES: usize = AAC_FRAME_SAMPLES * AAC_CHANNELS;
        let push_started = Instant::now();
        let mut encode_time = Duration::ZERO;

        let packet = Packet::new(
            0,
            TimeBase::new(1, i64::from(self.sample_rate)),
            data.to_vec(),
        )
        .with_pts(pts);
        self.decoder.send_packet(&packet).map_err(media_error)?;
        let mut samples = Vec::new();
        loop {
            match self.decoder.receive_frame() {
                Ok(DecodedFrame::Audio(audio)) => {
                    let Some(pcm) = audio.data.first() else {
                        return Err("E-AC-3 decoder returned no PCM samples".to_owned());
                    };
                    if pcm.len() != audio.samples as usize * AAC_CHANNELS * 4 {
                        return Err("E-AC-3 decoder returned an unexpected PCM format".to_owned());
                    }
                    self.first_pts.get_or_insert(audio.pts.unwrap_or(pts));
                    self.pending_pcm.extend(pcm.chunks_exact(4).map(|bytes| {
                        (f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) * 32767.0)
                            .round()
                            .clamp(i16::MIN as f32, i16::MAX as f32) as i16
                    }));
                    let chunk_samples = if self.bounded_encoder {
                        AAC_BOUNDED_CHUNK_SAMPLES
                    } else {
                        HOP_SAMPLES
                    };
                    while self.pending_pcm.len().saturating_sub(self.pending_pcm_start)
                        >= chunk_samples
                    {
                        let start = self.pending_pcm_start;
                        let end = start + chunk_samples;
                        let pcm_chunk = self.pending_pcm[start..end].to_vec();
                        let encode_started = Instant::now();
                        if self.bounded_encoder {
                            samples.extend(self.encode_bounded_chunk(&pcm_chunk)?);
                        } else {
                            let frame = self
                                .encoder
                                .as_mut()
                                .ok_or_else(|| "AAC encoder is not available".to_owned())?
                                .encode_frame(&pcm_chunk)
                                .map_err(media_error)?;
                            if let Some(sample) = self.aac_sample(frame)? {
                                samples.push(sample);
                            }
                        }
                        let frame_encode_time = encode_started.elapsed();
                        encode_time += frame_encode_time;
                        self.aac_encode_time += frame_encode_time;
                        self.pending_pcm_start = end;
                    }
                    if self.pending_pcm_start == self.pending_pcm.len() {
                        self.pending_pcm.clear();
                        self.pending_pcm_start = 0;
                    } else if self.pending_pcm_start >= chunk_samples {
                        let remaining = self.pending_pcm.len() - self.pending_pcm_start;
                        self.pending_pcm.copy_within(self.pending_pcm_start.., 0);
                        self.pending_pcm.truncate(remaining);
                        self.pending_pcm_start = 0;
                    }
                }
                Ok(_) => return Err("E-AC-3 decoder returned a non-audio frame".to_owned()),
                Err(oxideav_core::Error::NeedMore) => break,
                Err(error) => return Err(media_error(error)),
            }
        }
        self.audio_decode_pcm_time += push_started.elapsed().saturating_sub(encode_time);
        self.audio_packets_since_report += 1;
        if self.audio_packets_since_report >= 100 {
            tracing::info!(
                audio_packets = self.audio_packets_since_report,
                decode_and_pcm_ms = self.audio_decode_pcm_time.as_secs_f64() * 1000.0,
                aac_encode_ms = self.aac_encode_time.as_secs_f64() * 1000.0,
                "Plex audio transcode timing"
            );
            self.audio_decode_pcm_time = Duration::ZERO;
            self.aac_encode_time = Duration::ZERO;
            self.audio_packets_since_report = 0;
        }
        Ok(samples)
    }

    fn finish(&mut self) -> Result<Vec<Sample>, String> {
        let mut samples = Vec::new();
        if self.pending_pcm_start < self.pending_pcm.len() {
            let pcm = self.pending_pcm[self.pending_pcm_start..].to_vec();
            if self.bounded_encoder {
                samples.extend(self.encode_bounded_chunk(&pcm)?);
            } else {
                if let Some(sample) = self.encode_hop(&pcm)? {
                    samples.push(sample);
                }
            }
        }
        self.pending_pcm.clear();
        self.pending_pcm_start = 0;
        if let Some(encoder) = self.encoder.as_mut() {
            let frame = encoder.finish().map_err(media_error)?;
            if let Some(sample) = self.aac_sample(frame)? {
                samples.push(sample);
            }
        }
        Ok(samples)
    }

    fn encode_bounded_chunk(&mut self, pcm: &[i16]) -> Result<Vec<Sample>, String> {
        let base_pts = self.first_pts.unwrap_or(0).saturating_add(
            i64::try_from(self.encoded_frames)
                .unwrap_or(i64::MAX)
                .saturating_mul(AAC_FRAME_SAMPLES as i64),
        );
        let samples = Self::encode_aac_chunk(
            pcm,
            &mut self.pcm_preroll,
            self.sample_rate,
            base_pts,
        )?;
        self.encoded_frames = self.encoded_frames.saturating_add(samples.len() as u64);
        Ok(samples)
    }

    fn encode_aac_chunk(
        pcm: &[i16],
        preroll: &mut Vec<i16>,
        sample_rate: u32,
        base_pts: i64,
    ) -> Result<Vec<Sample>, String> {
        if pcm.len() % AAC_CHANNELS != 0 {
            return Err("AAC PCM chunk has an incomplete sample frame".to_owned());
        }
        let preroll_len = AAC_FRAME_SAMPLES * AAC_CHANNELS;
        let mut input = Vec::with_capacity(preroll_len + pcm.len());
        if preroll.is_empty() {
            input.resize(preroll_len, 0.0_f32);
        } else {
            input.extend(preroll.iter().map(|sample| f32::from(*sample) / 32768.0));
        }
        input.extend(pcm.iter().map(|sample| f32::from(*sample) / 32768.0));

        let mut encoder = rusty_aac::encode::AacEncoder::new(
            rusty_aac::encode::AacEncoderConfig {
                bitrate_bps: 128_000,
                ..Default::default()
            },
        );
        encoder
            .push_pcm(&input, AAC_CHANNELS as u16, sample_rate)
            .map_err(media_error)?;
        encoder.finish();

        let valid_samples = pcm.len() / AAC_CHANNELS;
        let mut samples = Vec::new();
        loop {
            let packet = match encoder.next_packet() {
                Ok(packet) => packet,
                Err(rusty_aac::Error::Eof) => break,
                Err(error) => return Err(media_error(error)),
            };
            let relative_pts = packet.pts.saturating_sub(AAC_FRAME_SAMPLES as i64);
            if relative_pts < 0 || relative_pts >= valid_samples as i64 {
                continue;
            }
            if packet.data.is_empty() || packet.duration == 0 {
                return Err("AAC encoder returned an empty access unit".to_owned());
            }
            let pts = base_pts.saturating_add(relative_pts);
            samples.push(Sample::new(
                Bytes::from(packet.data),
                Some(pts),
                Some(pts),
                Some(packet.duration),
                true,
            ));
        }

        if pcm.len() >= preroll_len {
            preroll.clear();
            preroll.extend_from_slice(&pcm[pcm.len() - preroll_len..]);
        }
        Ok(samples)
    }

    fn encode_hop(&mut self, pcm: &[i16]) -> Result<Option<Sample>, String> {
        let frame = self
            .encoder
            .as_mut()
            .ok_or_else(|| "AAC encoder is not available".to_owned())?
            .encode_frame(pcm)
            .map_err(media_error)?;
        self.aac_sample(frame)
    }

    fn aac_sample(&mut self, frame: Vec<u8>) -> Result<Option<Sample>, String> {
        let header_len = match frame.get(1) {
            Some(second) if second & 1 != 0 => 7,
            Some(_) => 9,
            None => return Err("AAC encoder returned a truncated ADTS frame".to_owned()),
        };
        if frame.len() < header_len || frame[0] != 0xff || frame[1] & 0xf6 != 0xf0 {
            return Err("AAC encoder returned an invalid ADTS frame".to_owned());
        }
        let frame_len = (usize::from(frame[3] & 0x03) << 11)
            | (usize::from(frame[4]) << 3)
            | usize::from(frame[5] >> 5);
        if frame_len != frame.len() || frame_len <= header_len {
            return Err("AAC encoder returned an incomplete ADTS frame".to_owned());
        }
        let frame_index = self.encoded_frames;
        self.encoded_frames = self.encoded_frames.saturating_add(1);
        if frame_index == 0 {
            return Ok(None);
        }
        let pts = self
            .first_pts
            .unwrap_or(0)
            .saturating_add((frame_index.saturating_sub(1) as i64).saturating_mul(1024));
        Ok(Some(Sample::new(
            Bytes::copy_from_slice(&frame[header_len..]),
            Some(pts),
            Some(pts),
            Some(1024),
            true,
        )))
    }



}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avc_sync_samples_detect_idr_nal_units() {
        let idr = [0, 0, 0, 2, 0x65, 0x88];
        let inter_frame = [0, 0, 0, 2, 0x41, 0x88];
        let malformed = [0, 0, 0, 4, 0x65, 0x88];

        assert!(is_video_sync_sample("V_MPEG4/ISO/AVC", &idr, 4));
        assert!(!is_avc_sync_sample(&inter_frame, 4));
        assert!(!is_avc_sync_sample(&malformed, 4));
    }

    #[test]
    fn avc_nal_length_size_comes_from_decoder_configuration() {
        assert_eq!(
            video_nal_length_size("V_MPEG4/ISO/AVC", &[1, 0, 0, 0, 0xff]),
            Ok(4)
        );
    }




    #[test]
    fn avc_configuration_box_parses_encoder_parameter_sets() {
        let mut encoder = H264Encoder::new(h264_encoder_config(32, 32, 2, 1_000_000, 24.0))
            .unwrap();
        let first = encoder.try_encode(&YuvFrame::black(32, 32)).unwrap();
        let (sps, pps) = extract_avc_parameter_sets(&first).unwrap();
        let mut private = vec![1, sps[1], sps[2], sps[3], 0xff, 0xe1];
        private.extend_from_slice(&(sps.len() as u16).to_be_bytes());
        private.extend_from_slice(&sps);
        private.push(1);
        private.extend_from_slice(&(pps.len() as u16).to_be_bytes());
        private.extend_from_slice(&pps);

        let config = AVCConfigurationBox::parse_body(&private).unwrap();
        assert_eq!(config.config.length_size_minus_one, 3);
    }


    #[test]
    fn manifests_reference_each_cached_fmp4_segment() {
        let directory = tempfile::tempdir().unwrap();
        let playback = RemuxedPlayback {
            directory,
            segments: vec![SegmentInfo {
                duration: 2.5,
                size: 1_000,
            }],
            progress: Arc::default(),
            video_codec: "hvc1.1.6.L93.B0".to_owned(),
            video_codec_id: "V_MPEGH/ISO/HEVC".to_owned(),
            audio_codec: Some("mp4a.40.2".to_owned()),
            width: 1280,
            height: 720,
            audio_segments: Vec::new(),
            audio_timings: Vec::new(),
            audio_sample_rate: Some(48_000),
            audio_channels: Some(2),
            separate_dash_tracks: false,
            matroska_hls: false,
            transcode_audio: false,
            video_timings: Vec::new(),
            video_transcode: None,
            video_start_segment: 0,
            video_segment_start_pts: Vec::new(),
            video_segment_start_ticks: Vec::new(),
            audio_start_segment: 0,
            video_start_pts: 0,
            video_decode_start_pts: 0,
            audio_start_pts: 0,
            audio_decode_start_pts: 0,
            audio_source_seek_pts: 0,
            source_seek_timestamp: 0,
            should_seek_source: false,
        };

        let hls = playback.hls_manifest("test-session");
        assert!(hls.contains(
            "#EXT-X-MAP:URI=\"/video/:/transcode/universal/session/test-session/init.mp4\""
        ));
        assert!(hls.contains("#EXTINF:2.500,"));
        assert!(hls.contains("/video/:/transcode/universal/session/test-session/segment-0.m4s"));
        assert_eq!(
            playback.resource("segment-0.m4s"),
            Some(playback.directory.path().join("segment-0.m4s"))
        );
        assert_eq!(
            playback.resource("0/header"),
            Some(playback.directory.path().join("init.mp4"))
        );
        assert_eq!(
            playback.resource("0/0.m4s"),
            Some(playback.directory.path().join("segment-0.m4s"))
        );
        assert!(playback.resource("segment-1.m4s").is_none());
        assert!(playback.resource("../segment-0.m4s").is_none());

        let dash = playback.dash_manifest("test-session");
        assert!(dash.contains("<SegmentTemplate timescale=\"1000\""));
        assert!(dash.contains("<SegmentTimeline><S d=\"2500\"/></SegmentTimeline>"));
        assert!(dash.contains("codecs=\"hvc1.1.6.L93.B0,mp4a.40.2\""));
        assert!(dash.contains("initialization=\"session/test-session/0/header\""));
        assert!(dash.contains("media=\"session/test-session/0/$Number$.m4s\""));
    }


    #[test]
    fn mkv_hls_manifest_references_initialization_and_cluster_resources() {
        let directory = tempfile::tempdir().unwrap();
        let playback = RemuxedPlayback {
            directory,
            segments: vec![SegmentInfo {
                duration: 2.25,
                size: 1_000,
            }],
            audio_segments: Vec::new(),
            progress: Arc::default(),
            video_codec: "hvc1.1.6.L93.B0".to_owned(),
            video_codec_id: "V_MPEGH/ISO/HEVC".to_owned(),
            audio_codec: Some("mp4a.40.2".to_owned()),
            width: 1280,
            height: 720,
            transcode_audio: true,
            video_timings: Vec::new(),
            audio_timings: Vec::new(),
            audio_sample_rate: Some(48_000),
            audio_channels: Some(2),
            separate_dash_tracks: false,
            matroska_hls: true,
            video_transcode: None,
            video_start_segment: 0,
            video_segment_start_pts: vec![0],
            video_segment_start_ticks: vec![0],
            audio_start_segment: 0,
            video_start_pts: 0,
            video_decode_start_pts: 0,
            audio_start_pts: 0,
            audio_decode_start_pts: 0,
            audio_source_seek_pts: 0,
            source_seek_timestamp: 0,
            should_seek_source: false,
        };

        let manifest = playback.hls_manifest("mkv-session");
        assert!(manifest.contains("/mkv-session/init.mkv"));
        assert!(manifest.contains("/mkv-session/segment-0.mkv"));
        assert_eq!(
            playback.resource("init.mkv"),
            Some(playback.directory.path().join("init.mkv"))
        );
        assert_eq!(
            playback.resource("segment-0.mkv"),
            Some(playback.directory.path().join("segment-0.mkv"))
        );
    }

    #[test]
    fn opus_audio_is_only_accepted_for_matroska_hls_copy() {
        let opus_private = b"OpusHead\x01\x02\x38\x01\x80\xbb\x00\x00\x00\x00\x00\x00";
        assert_eq!(
            opus_track_config(opus_private, 48_000, 2),
            Ok((48_000, 2, 6_500_000))
        );

        let mut source = mkv_hls::initialization_segment(
            mkv_hls::VideoTrack {
                codec_id: "V_MPEGH/ISO/HEVC",
                codec_private: &[0; 22],
                width: 640,
                height: 360,
            },
            Some(mkv_hls::AudioTrack {
                codec_id: "A_OPUS",
                codec_private: opus_private,
                sample_rate: 48_000,
                channels: 2,
                codec_delay: Some(6_500_000),
                seek_pre_roll: Some(80_000_000),
            }),
        )
        .unwrap();
        source.extend(
            mkv_hls::cluster_segment(
                0,
                &[
                    mkv_hls::Block {
                        track_number: 1,
                        timestamp_ms: 0,
                        data: &[0, 0, 0, 2, 0x26, 0x01],
                        keyframe: true,
                    },
                    mkv_hls::Block {
                        track_number: 2,
                        timestamp_ms: 0,
                        data: &[0xf8, 0xff, 0xfe],
                        keyframe: true,
                    },
                ],
            )
            .unwrap(),
        );
        let mut file = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(file.as_file_mut(), &source).unwrap();

        assert_eq!(
            RemuxedPlayback::validate_source(file.path(), false, true).unwrap(),
            Some("opus".to_owned())
        );
        let error = RemuxedPlayback::validate_source(file.path(), false, false).unwrap_err();
        assert!(error.contains("A_OPUS"));
        assert!(RemuxedPlayback::validate_source(file.path(), true, true).is_err());
    }

    #[test]
    fn dash_manifest_uses_separate_audio_and_video_tracks() {
        let directory = tempfile::tempdir().unwrap();
        let playback = RemuxedPlayback {
            directory,
            segments: vec![
                SegmentInfo {
                    duration: 2.0,
                    size: 1_000_000,
                },
                SegmentInfo {
                    duration: 2.0,
                    size: 1_000_000,
                },
            ],
            audio_segments: vec![
                SegmentInfo {
                    duration: 2.005,
                    size: 32_000,
                },
                SegmentInfo {
                    duration: 2.005,
                    size: 32_000,
                },
            ],
            progress: Arc::default(),
            video_codec: "avc1.42E029".to_owned(),
            video_codec_id: "V_MPEG4/ISO/AVC".to_owned(),
            audio_codec: Some("mp4a.40.2".to_owned()),
            width: 1280,
            height: 720,
            transcode_audio: true,
            video_timings: Vec::new(),
            audio_timings: Vec::new(),
            audio_sample_rate: Some(48_000),
            audio_channels: Some(2),
            separate_dash_tracks: true,
            matroska_hls: false,
            video_transcode: None,
            video_start_segment: 1,
            video_segment_start_pts: Vec::new(),
            video_segment_start_ticks: Vec::new(),
            audio_start_segment: 1,
            video_start_pts: 0,
            video_decode_start_pts: 0,
            audio_start_pts: 0,
            audio_decode_start_pts: 0,
            audio_source_seek_pts: 0,
            source_seek_timestamp: 0,
            should_seek_source: false,
        };
        let manifest = playback.dash_manifest("split-session");
        assert_eq!(manifest.matches("<AdaptationSet ").count(), 2);
        assert!(manifest.contains("contentType=\"video\""));
        assert!(manifest.contains("codecs=\"avc1.42E029\""));
        assert!(manifest.contains("contentType=\"audio\""));
        assert!(manifest.contains("codecs=\"mp4a.40.2\""));
        assert!(!manifest.contains("avc1.42E029,mp4a.40.2"));
        assert!(manifest.contains("initialization=\"session/split-session/0/header\""));
        assert!(manifest.contains("initialization=\"session/split-session/1/header\""));
        assert_eq!(
            playback.resource("0/header"),
            Some(playback.directory.path().join("init-video.mp4"))
        );
        assert_eq!(
            playback.resource("1/header"),
            Some(playback.directory.path().join("init-audio.mp4"))
        );
        assert!(playback.resource("0/0.m4s").is_none());
        assert!(playback.resource("1/0.m4s").is_none());
        assert_eq!(
            playback.resource("0/1.m4s"),
            Some(playback.directory.path().join("video-segment-1.m4s"))
        );
        assert_eq!(
            playback.resource("1/1.m4s"),
            Some(playback.directory.path().join("audio-segment-1.m4s"))
        );
        assert!(manifest.contains("startNumber=\"0\""));
    }

    #[tokio::test]
    async fn waits_for_background_segment_creation() {
        let directory = tempfile::tempdir().unwrap();
        let playback = Arc::new(RemuxedPlayback {
            directory,
            segments: vec![SegmentInfo {
                duration: 2.5,
                size: 1_000,
            }],
            progress: Arc::default(),
            video_codec: "hvc1.1.6.L93.B0".to_owned(),
            video_codec_id: "V_MPEGH/ISO/HEVC".to_owned(),
            audio_codec: Some("mp4a.40.2".to_owned()),
            width: 1280,
            height: 720,
            audio_segments: Vec::new(),
            audio_timings: Vec::new(),
            audio_sample_rate: Some(48_000),
            audio_channels: Some(2),
            separate_dash_tracks: false,
            matroska_hls: false,
            transcode_audio: false,
            video_timings: Vec::new(),
            video_transcode: None,
            video_start_segment: 0,
            video_segment_start_pts: Vec::new(),
            video_segment_start_ticks: Vec::new(),
            audio_start_segment: 0,
            video_start_pts: 0,
            video_decode_start_pts: 0,
            audio_start_pts: 0,
            audio_decode_start_pts: 0,
            audio_source_seek_pts: 0,
            source_seek_timestamp: 0,
            should_seek_source: false,
        });
        let cache = playback.directory.path().to_owned();
        let progress = Arc::clone(&playback.progress);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            fs::write(cache.join("segment-0.m4s"), b"segment").unwrap();
            progress.changed.notify_waiters();
        });

        let path = playback.wait_resource("segment-0.m4s").await.unwrap();
        assert!(path.is_file());
    }

    #[test]
    fn aac_encoder_matches_mp4_audio_configuration() {
        let sample_rate = 48_000;
        let config = transmux::rtp_sdp::aac_config_from_asc_bytes(
            rusty_aac::encode::audio_specific_config_bytes(sample_rate, 2),
        )
        .unwrap();
        assert!(matches!(
            config,
            CodecConfig::Aac {
                channel_count: 2,
                sample_rate: 48_000,
                ..
            }
        ));

        let pcm = (0..AAC_BOUNDED_CHUNK_SAMPLES)
            .map(|index| ((index as f32 * 0.037).sin() * 12_000.0) as i16)
            .collect::<Vec<_>>();
        let mut preroll = Vec::new();
        let first_base_pts = 12_345;
        let first = Eac3AacTranscoder::encode_aac_chunk(
            &pcm,
            &mut preroll,
            sample_rate,
            first_base_pts,
        )
        .unwrap();
        assert_eq!(first.len(), AAC_BOUNDED_CHUNK_FRAMES);
        assert_eq!(first[0].pts, Some(first_base_pts));
        assert_eq!(first.last().unwrap().duration, Some(AAC_FRAME_SAMPLES as u32));

        let second_base_pts = first_base_pts + AAC_BOUNDED_CHUNK_FRAMES as i64 * 1024;
        let second = Eac3AacTranscoder::encode_aac_chunk(
            &pcm,
            &mut preroll,
            sample_rate,
            second_base_pts,
        )
        .unwrap();
        assert_eq!(second.len(), AAC_BOUNDED_CHUNK_FRAMES);
        assert_eq!(second[0].pts, Some(second_base_pts));
        assert_eq!(
            second[0].pts,
            Some(first.last().unwrap().pts.unwrap() + 1024)
        );
        let mut decoder = rusty_aac::decode::Decoder::new(sample_rate);
        for sample in first.iter().chain(&second) {
            decoder
                .decode(sample.data.as_ref(), sample.pts)
                .unwrap();
        }

    }

    #[test]
    fn rejects_non_matroska_input() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), b"not a Matroska file").unwrap();
        assert!(RemuxedPlayback::remux_with_audio_transcoding(file.path(), false).is_err());
    }


    #[test]
    fn pure_rust_h264_encoder_outputs_muxable_gop_samples() {
        let mut encoder = H264Encoder::new(h264_encoder_config(32, 32, 2, 1_000_000, 24.0))
            .unwrap();
        let frame = YuvFrame::black(32, 32);
        let first = encoder.try_encode(&frame).unwrap();
        let (sps, pps) = extract_avc_parameter_sets(&first).unwrap();
        assert_eq!(sps[0] & 0x1f, 7);
        assert_eq!(pps[0] & 0x1f, 8);
        let (first_sample, first_sync) = annex_b_to_avcc_sample(&first).unwrap().unwrap();
        assert!(first_sync);
        assert!(!first_sample.is_empty());

        let second = encoder.try_encode(&frame).unwrap();
        assert!(!annex_b_to_avcc_sample(&second).unwrap().unwrap().1);
        let third = encoder.try_encode(&frame).unwrap();
        assert!(annex_b_to_avcc_sample(&third).unwrap().unwrap().1);

        let timings = (0..48)
            .map(|index| VideoFrameTiming {
                pts: i64::from(index) * 3_750,
                duration: 3_750,
            })
            .collect::<Vec<_>>();
        let config = VideoTranscodeConfig::prepare(32, 32, &timings, 1_000_000).unwrap();
        assert_eq!(config.gop_size, 48);
        assert!(config.codec.starts_with("avc1."));
        assert!(matches!(
            config.codec_config(32, 32),
            CodecConfig::Avc { width: 32, height: 32, .. }
        ));
    }

    #[test]
    fn transcode_segment_timeline_follows_fixed_idr_gops() {
        let timings = (0..150)
            .map(|index| VideoFrameTiming {
                pts: i64::from(index) * 3_000,
                duration: 3_000,
            })
            .collect::<Vec<_>>();
        assert_eq!(transcode_segment_durations(&timings, 60).unwrap(), [2.0, 2.0, 1.0]);
    }


    #[test]
    fn audio_segment_timeline_uses_aac_frame_boundaries() {
        let timings = (0..200)
            .map(|index| AudioFrameTiming {
                pts: i64::from(index) * 1024,
                duration: 1024,
            })
            .collect::<Vec<_>>();
        let durations = audio_segment_durations(&timings, 48_000);
        assert_eq!(durations.len(), 3);
        assert!((durations[0] - 94.0 * 1024.0 / 48_000.0).abs() < 0.0001);
        assert!((durations[1] - 94.0 * 1024.0 / 48_000.0).abs() < 0.0001);
        assert!((durations[2] - 12.0 * 1024.0 / 48_000.0).abs() < 0.0001);
    }


    #[test]
    fn offsets_select_full_timeline_segment_boundaries() {
        let durations = [2.0, 2.5, 3.0];
        assert_eq!(segment_index_for_offset(&durations, 0.0), 0);
        assert_eq!(segment_index_for_offset(&durations, 2.0), 1);
        assert_eq!(segment_index_for_offset(&durations, 4.49), 1);
        assert_eq!(segment_index_for_offset(&durations, 4.5), 2);

        let audio_timings = (0..200)
            .map(|index| AudioFrameTiming {
                pts: i64::from(index) * 1024,
                duration: 1024,
            })
            .collect::<Vec<_>>();
        assert_eq!(audio_segment_start_pts(&audio_timings, 48_000, 1), Some(94 * 1024));
        assert_eq!(audio_segment_start_pts(&audio_timings, 48_000, 2), Some(188 * 1024));
    }


    #[test]
    fn nonzero_offset_starts_at_matching_video_segment() {
        let durations = [2.0; 10];
        assert_eq!(playback_segment_start(&durations, 14.0), (true, 7));
        assert_eq!(playback_segment_start(&durations, 0.0), (false, 0));
    }


    #[test]
    fn audio_transcode_seek_preserves_aac_sample_phase() {
        let first_pts = 1_000;
        let timings = (0..600)
            .map(|index| AudioFrameTiming {
                pts: first_pts + i64::from(index) * 1024,
                duration: 1024,
            })
            .collect();
        let source_packets = (0..=390)
            .map(|index| {
                let sample_offset = index * 1536;
                AudioSourcePacket {
                    pts: first_pts + sample_offset as i64,
                    sample_offset,
                }
            })
            .collect();
        let scan = AudioScan {
            timings,
            source_packets,
            sample_rate: 48_000,
            channels: 2,
            codec_delay: 0,
            timestamp_scale: 1_000_000,
        };

        let (source_pts, output_pts) = audio_transcode_seek_point(&scan, first_pts + 10 * 48_000)
            .unwrap();
        assert_eq!(source_pts, output_pts);
        assert_eq!((output_pts - first_pts) % 1024, 0);
        assert_eq!(output_pts, first_pts + 384_000);
    }


    #[test]
    fn idle_playback_builds_are_detected_and_refreshed() {
        let progress = PlaybackProgress::default();
        assert!(!progress.is_idle_for(BUILD_IDLE_TIMEOUT));
        *progress.last_request.lock().unwrap() = Instant::now() - BUILD_IDLE_TIMEOUT;
        assert!(progress.is_idle_for(BUILD_IDLE_TIMEOUT));
        progress.touch();
        assert!(!progress.is_idle_for(BUILD_IDLE_TIMEOUT));
    }


    #[test]
    fn muxed_build_waits_until_client_demand_advances() {
        let progress = Arc::new(PlaybackProgress::default());
        let limiter = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::clone(&limiter).try_acquire_owned().unwrap();
        progress
            .request_resource("segment-0.m4s", false)
            .unwrap();
        let worker_progress = Arc::clone(&progress);
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut permit = Some(permit);
            sender
                .send(worker_progress.wait_for_capacity(
                    4,
                    0,
                    0,
                    0,
                    false,
                    false,
                    Duration::from_secs(2),
                    &limiter,
                    &mut permit,
                ))
                .unwrap();
        });

        assert!(receiver.recv_timeout(Duration::from_millis(20)).is_err());
        progress
            .request_resource("segment-1.m4s", false)
            .unwrap();
        assert!(receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .is_ok());
        worker.join().unwrap();
    }


    #[test]
    fn paused_build_releases_limiter_until_demand_advances() {
        let progress = Arc::new(PlaybackProgress::default());
        progress.request_resource("segment-0.m4s", false).unwrap();
        let limiter = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::clone(&limiter).try_acquire_owned().unwrap();
        let worker_progress = Arc::clone(&progress);
        let worker_limiter = Arc::clone(&limiter);
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut permit = Some(permit);
            sender
                .send(worker_progress.wait_for_capacity(
                    4,
                    0,
                    0,
                    0,
                    false,
                    false,
                    Duration::from_secs(2),
                    &worker_limiter,
                    &mut permit,
                ))
                .unwrap();
        });

        let deadline = Instant::now() + Duration::from_secs(1);
        let other_permit = loop {
            if let Ok(permit) = Arc::clone(&limiter).try_acquire_owned() {
                break permit;
            }
            assert!(Instant::now() < deadline, "paused build kept the limiter permit");
            std::thread::yield_now();
        };
        progress.request_resource("segment-1.m4s", false).unwrap();
        assert!(receiver.recv_timeout(Duration::from_millis(20)).is_err());
        drop(other_permit);
        assert!(receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .is_ok());
        worker.join().unwrap();
    }

    #[test]
    fn separate_dash_build_waits_for_demand_on_both_tracks() {
        let progress = Arc::new(PlaybackProgress::default());
        let limiter = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::clone(&limiter).try_acquire_owned().unwrap();
        progress.request_resource("0/0.m4s", true).unwrap();
        progress.request_resource("1/0.m4s", true).unwrap();
        let worker_progress = Arc::clone(&progress);
        let (sender, receiver) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut permit = Some(permit);
            sender
                .send(worker_progress.wait_for_capacity(
                    4,
                    4,
                    0,
                    0,
                    true,
                    true,
                    Duration::from_secs(2),
                    &limiter,
                    &mut permit,
                ))
                .unwrap();
        });

        progress.request_resource("0/1.m4s", true).unwrap();
        assert!(receiver.recv_timeout(Duration::from_millis(20)).is_err());
        progress.request_resource("1/1.m4s", true).unwrap();
        assert!(receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .is_ok());
        worker.join().unwrap();
    }

    #[test]
    fn paused_build_stops_after_client_becomes_idle() {
        let progress = PlaybackProgress::default();
        progress
            .request_resource("segment-0.m4s", false)
            .unwrap();
        *progress.last_request.lock().unwrap() =
            Instant::now() - Duration::from_millis(20);

        let limiter = Arc::new(tokio::sync::Semaphore::new(1));
        let mut permit = Some(Arc::clone(&limiter).try_acquire_owned().unwrap());
        let error = progress
            .wait_for_capacity(
                4,
                0,
                0,
                0,
                false,
                false,
                Duration::from_millis(10),
                &limiter,
                &mut permit,
            )
            .unwrap_err();
        assert_eq!(error, "playback build stopped after the client became idle");
    }


    #[test]
    fn hls_build_uses_the_first_requested_segment() {
        let progress = PlaybackProgress::default();
        progress.request_resource("init.mp4", false).unwrap();
        progress.request_resource("segment-7.m4s", false).unwrap();
        progress.request_resource("segment-9.m4s", false).unwrap();

        let limiter = Arc::new(tokio::sync::Semaphore::new(1));
        let mut permit = Some(Arc::clone(&limiter).try_acquire_owned().unwrap());
        let first_segment = progress
            .wait_for_initial_muxed_segment(Duration::from_secs(1), &limiter, &mut permit)
            .unwrap();

        assert_eq!(first_segment, 7);
    }

    #[test]
    fn mkv_hls_build_uses_the_first_requested_segment() {
        let progress = PlaybackProgress::default();
        progress.request_resource("init.mkv", false).unwrap();
        progress.request_resource("segment-25.mkv", false).unwrap();

        let limiter = Arc::new(tokio::sync::Semaphore::new(1));
        let mut permit = Some(Arc::clone(&limiter).try_acquire_owned().unwrap());
        let first_segment = progress
            .wait_for_initial_muxed_segment(Duration::from_secs(1), &limiter, &mut permit)
            .unwrap();

        assert_eq!(first_segment, 25);
    }


    #[test]
    fn hls_seek_point_uses_requested_source_boundary() {
        let starts = hls_segment_start(
            7,
            0,
            &[0, 10, 20, 30, 40, 50, 60, 70],
            &[0, 100, 200, 300, 400, 500, 600, 700],
        )
        .unwrap();
        assert_eq!(starts, (7, 70, 700));

        let prepared = hls_segment_start(
            2,
            5,
            &[0, 10, 20, 30, 40, 50],
            &[0, 100, 200, 300, 400, 500],
        )
        .unwrap();
        assert_eq!(prepared, (5, 50, 500));
    }

    #[test]
    fn hls_seek_point_rejects_missing_source_boundary() {
        assert!(hls_segment_start(8, 0, &[0, 10], &[0, 100]).is_err());
    }


    #[test]
    fn direct_hls_playback_filters_frames_without_matroska_seek() {
        assert!(!should_seek_matroska_source(true, false, false));
        assert!(should_seek_matroska_source(true, true, false));
        assert!(should_seek_matroska_source(true, false, true));
        assert!(!should_seek_matroska_source(false, true, true));
    }
}
