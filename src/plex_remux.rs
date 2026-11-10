use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
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

const VIDEO_TRACK_ID: u32 = 1;
const AUDIO_TRACK_ID: u32 = 2;
const VIDEO_TIMESCALE: u32 = 90_000;
const SEGMENT_TARGET_SECONDS: f64 = 2.0;
const MAX_PENDING_VIDEO_TICKS: i64 = VIDEO_TIMESCALE as i64 * 30;
const BUILD_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

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

struct BuiltSegments {
    video: Vec<SegmentInfo>,
    audio: Vec<SegmentInfo>,
}

struct VideoScan {
    segment_durations: Vec<f64>,
    segment_start_ticks: Vec<u64>,
    segment_start_pts: Vec<i64>,
    frame_timings: Vec<VideoFrameTiming>,
    codec: String,
    width: u16,
    height: u16,
}

#[derive(Default)]
struct PlaybackProgressState {
    complete: bool,
    error: Option<String>,
}

struct PlaybackProgress {
    state: Mutex<PlaybackProgressState>,
    changed: tokio::sync::Notify,
    last_request: Mutex<Instant>,
}

impl Default for PlaybackProgress {
    fn default() -> Self {
        Self {
            state: Mutex::default(),
            changed: tokio::sync::Notify::new(),
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
}

pub struct RemuxedPlayback {
    directory: TempDir,
    segments: Vec<SegmentInfo>,
    audio_segments: Vec<SegmentInfo>,
    progress: Arc<PlaybackProgress>,
    video_codec: String,
    audio_codec: Option<String>,
    width: u16,
    height: u16,
    transcode_audio: bool,
    video_timings: Vec<VideoFrameTiming>,
    audio_timings: Vec<AudioFrameTiming>,
    audio_sample_rate: Option<u32>,
    audio_channels: Option<u16>,
    separate_dash_tracks: bool,
    video_transcode: Option<VideoTranscodeConfig>,
    video_start_segment: usize,
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
    pub fn prepare(
        path: &Path,
        transcode_audio: bool,
        transcode_video: bool,
        separate_dash_tracks: bool,
        offset_seconds: f64,
    ) -> Result<Self, String> {
        let directory = tempfile::Builder::new()
            .prefix("harbringer-playback-")
            .tempdir()
            .map_err(|error| format!("create playback cache: {error}"))?;
        let scan = Self::scan_video_segments(path)?;
        if scan.segment_durations.is_empty() {
            return Err("media did not contain a complete playback segment".to_owned());
        }
        let audio_codec = Self::validate_source(path, transcode_audio)?;
        let source_size = fs::metadata(path).map_err(media_error)?.len();
        let source_duration = scan.segment_durations.iter().sum::<f64>();
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
        let should_seek_source = separate_dash_tracks
            && offset_seconds.is_finite()
            && offset_seconds > 0.0;
        if !offset_seconds.is_finite() || offset_seconds < 0.0 {
            return Err("playback offset must be a non-negative number of seconds".to_owned());
        }
        if offset_seconds >= source_duration {
            return Err("playback offset is outside the source duration".to_owned());
        }
        let video_start_segment = if should_seek_source {
            segment_index_for_offset(&segment_durations, offset_seconds)
        } else {
            0
        };
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
            audio_codec,
            width: scan.width,
            height: scan.height,
            transcode_audio,
            video_timings: scan.frame_timings,
            audio_timings: audio_scan.as_ref().map_or_else(Vec::new, |scan| scan.timings.clone()),
            audio_sample_rate: audio_scan.as_ref().map(|scan| scan.sample_rate),
            audio_channels: audio_scan.as_ref().map(|scan| scan.channels),
            separate_dash_tracks,
            video_transcode,
            video_start_segment,
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
        let playback = Self::prepare(path, transcode_audio, false, false, 0.0)?;
        playback.build(path)?;
        Ok(playback)
    }

    pub fn build(&self, path: &Path) -> Result<(), String> {
        let result = remux_matroska(
            path,
            self.directory.path(),
            self.transcode_audio,
            self.video_transcode.as_ref(),
            &self.video_timings,
            &self.audio_timings,
            self.audio_sample_rate,
            self.separate_dash_tracks,
            self.video_start_segment,
            self.audio_start_segment,
            self.video_start_pts,
            self.video_decode_start_pts,
            self.audio_start_pts,
            self.audio_decode_start_pts,
            self.audio_source_seek_pts,
            self.source_seek_timestamp,
            self.should_seek_source,
            &|| self.progress.is_idle_for(BUILD_IDLE_TIMEOUT),
            &mut |_name| self.progress.changed.notify_waiters(),
        );
        let result = result.and_then(|mut actual| {
            actual.video[..self.video_start_segment]
                .clone_from_slice(&self.segments[..self.video_start_segment]);
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
        self.progress.touch();
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

    fn validate_source(path: &Path, transcode_audio: bool) -> Result<Option<String>, String> {
        let source =
            MatroskaFile::open(File::open(path).map_err(media_error)?).map_err(media_error)?;
        let audio = source
            .tracks()
            .iter()
            .find(|track| track.track_type() == TrackType::Audio && track.codec_id() == "A_EAC3");
        if source
            .tracks()
            .iter()
            .any(|track| track.track_type() == TrackType::Audio)
            && audio.is_none()
        {
            return Err("source audio codec is not supported for remuxing".to_owned());
        }
        match audio {
            Some(track) if transcode_audio => {
                let sample_rate = track
                    .audio()
                    .ok_or_else(|| "E-AC-3 track has no audio settings".to_owned())?
                    .sampling_frequency()
                    .round() as u32;
                Eac3AacTranscoder::new(sample_rate, None)?;
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
            "#EXTM3U\n#EXT-X-VERSION:7\n#EXT-X-TARGETDURATION:{target_duration}\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXT-X-MAP:URI=\"/video/:/transcode/universal/session/{session_id}/init.mp4\"\n"
        );
        for (index, segment) in self.segments.iter().enumerate() {
            output.push_str(&format!(
                "#EXTINF:{:.3},\n/video/:/transcode/universal/session/{session_id}/segment-{index}.m4s\n",
                segment.duration
            ));
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
                track.track_type() == TrackType::Video && track.codec_id() == "V_MPEGH/ISO/HEVC"
            })
            .ok_or_else(|| "source has no remux-supported HEVC video track".to_owned())?;
        let video_number = video.track_number().get();
        let video_private = video
            .codec_private()
            .ok_or_else(|| "HEVC track has no decoder configuration".to_owned())?;
        let hevc_config = HEVCConfigurationBox::parse_body(video_private).map_err(media_error)?;
        let codec = hevc_config.config.rfc6381();
        let video_settings = video
            .video()
            .ok_or_else(|| "HEVC track has no video settings".to_owned())?;
        let width = u16::try_from(video_settings.pixel_width().get())
            .map_err(|_| "HEVC video width is outside the CMAF range".to_owned())?;
        let height = u16::try_from(video_settings.pixel_height().get())
            .map_err(|_| "HEVC video height is outside the CMAF range".to_owned())?;
        let nal_length_size = usize::from(
            *video_private
                .get(21)
                .ok_or_else(|| "HEVC decoder configuration is too short".to_owned())?
                & 0x03,
        ) + 1;
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
                .ok_or_else(|| "HEVC track has no frame duration metadata".to_owned())?;
            let pts = ns_to_ticks(
                frame_timestamp_ns(frame.timestamp, timestamp_scale)?.saturating_sub(codec_delay),
                VIDEO_TIMESCALE,
            )?;
            let duration = ns_to_duration(duration_ns, VIDEO_TIMESCALE)?;
            frame_timings.push(VideoFrameTiming { pts, duration });
            let is_sync = frame
                .is_keyframe
                .unwrap_or_else(|| is_hevc_sync_sample(&frame.data, nal_length_size));
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

        let start = start.ok_or_else(|| "HEVC track has no sync sample".to_owned())?;
        let end = last_video_end.ok_or_else(|| "HEVC track has no frames".to_owned())?;
        durations.push(ticks_to_seconds(end.saturating_sub(start), VIDEO_TIMESCALE).max(0.001));
        frame_timings.sort_by_key(|timing| timing.pts);
        Ok(VideoScan {
            segment_durations: durations,
            segment_start_ticks,
            segment_start_pts,
            frame_timings,
            codec,
            width,
            height,
        })
    }
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
    video_timings: &[VideoFrameTiming],
    audio_timings: &[AudioFrameTiming],
    audio_sample_rate_hint: Option<u32>,
    separate_dash_tracks: bool,
    video_start_segment: usize,
    audio_start_segment: usize,
    video_start_pts: i64,
    video_decode_start_pts: i64,
    audio_start_pts: i64,
    audio_decode_start_pts: i64,
    audio_source_seek_pts: i64,
    source_seek_timestamp: u64,
    should_seek_source: bool,
    should_cancel: &impl Fn() -> bool,
    on_resource_ready: &mut impl FnMut(&str),
) -> Result<BuiltSegments, String> {
    let mut probe =
        MatroskaFile::open(File::open(path).map_err(media_error)?).map_err(media_error)?;
    let timestamp_scale = probe.info().timestamp_scale().get();
    let video_track = probe
        .tracks()
        .iter()
        .find(|track| {
            track.track_type() == TrackType::Video && track.codec_id() == "V_MPEGH/ISO/HEVC"
        })
        .ok_or_else(|| "source has no remux-supported HEVC video track".to_owned())?;
    let audio_track = probe
        .tracks()
        .iter()
        .find(|track| track.track_type() == TrackType::Audio && track.codec_id() == "A_EAC3");
    let has_audio = probe
        .tracks()
        .iter()
        .any(|track| track.track_type() == TrackType::Audio);
    if has_audio && audio_track.is_none() {
        return Err("source audio codec is not supported for remuxing".to_owned());
    }

    let video_number = video_track.track_number().get();
    let video_private = video_track
        .codec_private()
        .ok_or_else(|| "HEVC track has no decoder configuration".to_owned())?
        .to_vec();
    let nal_length_size = usize::from(
        *video_private
            .get(21)
            .ok_or_else(|| "HEVC decoder configuration is too short".to_owned())?
            & 0x03,
    ) + 1;
    let video_settings = video_track
        .video()
        .ok_or_else(|| "HEVC track has no video settings".to_owned())?;
    let width = u16::try_from(video_settings.pixel_width().get())
        .map_err(|_| "HEVC video width is outside the CMAF range".to_owned())?;
    let height = u16::try_from(video_settings.pixel_height().get())
        .map_err(|_| "HEVC video height is outside the CMAF range".to_owned())?;
    let video_default_duration = video_track
        .default_duration()
        .map(|duration| duration.get());
    let video_codec_delay = video_track.codec_delay().unwrap_or(0);

    let (audio_number, audio_spec, mut audio_transcoder) = if let Some(track) = audio_track {
        let number = track.track_number().get();
        let audio_settings = track
            .audio()
            .ok_or_else(|| "E-AC-3 track has no audio settings".to_owned())?;
        let sample_rate = audio_settings.sampling_frequency().round() as u32;
        if transcode_audio {
            let asc = rusty_aac::encode::audio_specific_config_bytes(sample_rate, 2);
            let config = transmux::rtp_sdp::aac_config_from_asc_bytes(asc).map_err(media_error)?;
            let spec = TrackSpec::new(TrackId::new(AUDIO_TRACK_ID), sample_rate, config);
            let transcoder = Eac3AacTranscoder::new(
                sample_rate,
                should_seek_source.then_some(audio_decode_start_pts),
            )?;
            (Some(number), Some(spec), Some(transcoder))
        } else {
            let channels = u16::try_from(audio_settings.channels().get())
                .map_err(|_| "E-AC-3 channel count is outside the CMAF range".to_owned())?;
            let mut first_audio = None;
            let mut frame = Frame::default();
            while probe.next_frame(&mut frame).map_err(media_error)? {
                if frame.track == number {
                    first_audio = Some(frame.data.clone());
                    break;
                }
            }
            let first_audio =
                first_audio.ok_or_else(|| "E-AC-3 track has no samples".to_owned())?;
            let syncframes = Ec3SyncframeInfo::from_es_first_au(&first_audio);
            let config = Ec3SpecificBox::from_syncframes(&syncframes).map_err(media_error)?;
            let channels = u16::from(config.channel_count()).max(channels);
            let config = CodecConfig::Eac3 {
                config,
                channel_count: channels,
                sample_rate,
                sample_size: 16,
            };
            let spec = TrackSpec::new(TrackId::new(AUDIO_TRACK_ID), sample_rate, config);
            (Some(number), Some(spec), None)
        }
    } else {
        if transcode_audio {
            return Err(
                "audio transcoding was requested for a source with no E-AC-3 track".to_owned(),
            );
        }
        (None, None, None)
    };

    let hevc_config = HEVCConfigurationBox::parse_body(&video_private).map_err(media_error)?;
    let mut video_decoder = video_transcode
        .map(|_| hevc_decoder_from_config(&hevc_config))
        .transpose()?;
    let mut video_encoder = video_transcode
        .map(|config| config.encoder(width, height))
        .transpose()?;
    let video_config = if let Some(config) = video_transcode {
        config.codec_config(width, height)
    } else {
        CodecConfig::Hevc {
            config: hevc_config,
            width,
            height,
        }
    };
    let video_spec = TrackSpec::new(TrackId::new(VIDEO_TRACK_ID), VIDEO_TIMESCALE, video_config);
    let audio_sample_rate = audio_spec.as_ref().map(|spec| spec.timescale);
    if audio_sample_rate_hint.is_some_and(|expected| Some(expected) != audio_sample_rate) {
        return Err("audio sample rate changed between DASH preparation and remux".to_owned());
    }
    let mut segmenter = None;
    let mut video_segmenter = None;
    let mut audio_segmenter = None;
    if separate_dash_tracks {
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
    let mut audio_codec_delay = 0_u64;
    if let Some(number) = audio_number {
        audio_codec_delay = source
            .tracks()
            .iter()
            .find(|track| track.track_number().get() == number)
            .and_then(|track| track.codec_delay())
            .unwrap_or(0);
    }
    if should_seek_source {
        source
            .seek(source_seek_timestamp.saturating_sub(1))
            .map_err(media_error)?;
    }

    while source.next_frame(&mut frame).map_err(media_error)? {
        if should_cancel() {
            return Err("playback build stopped after the client became idle".to_owned());
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
                    .ok_or_else(|| "HEVC track has no frame duration metadata".to_owned())?;
                let pts = ns_to_ticks(
                    presentation_ns.saturating_sub(video_codec_delay),
                    VIDEO_TIMESCALE,
                )?;
                let duration = ns_to_duration(duration_ns, VIDEO_TIMESCALE)?;
                let dts = *video_dts.get_or_insert(pts);
                let first_dts = *video_first_dts.get_or_insert(dts);
                let segment_start = video_segment_start.unwrap_or(first_dts);
                if dts.saturating_sub(segment_start) > MAX_PENDING_VIDEO_TICKS {
                    return Err("HEVC GOP exceeds the bounded segment window".to_owned());
                }
                let is_sync = frame
                    .is_keyframe
                    .unwrap_or_else(|| is_hevc_sync_sample(&frame.data, nal_length_size));
                let sample = Sample::new(
                    Bytes::from(frame.data.clone()),
                    Some(dts),
                    Some(pts),
                    Some(duration),
                    is_sync,
                );
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
                video_dts = Some(dts.saturating_add(i64::from(duration)));
                last_video_end = Some(pts.saturating_add(i64::from(duration)));
            }
        } else if Some(frame.track) == audio_number {
            let Some(sample_rate) = audio_sample_rate else {
                continue;
            };
            let pts = ns_to_ticks(
                presentation_ns.saturating_sub(audio_codec_delay),
                sample_rate,
            )?;
            if should_seek_source && pts < audio_source_seek_pts {
                continue;
            }
            if let Some(transcoder) = audio_transcoder.as_mut() {
                for sample in transcoder.push(&frame.data, pts)? {
                    if separate_dash_tracks {
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
            if separate_dash_tracks {
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
    encoder: AacStreamEncoder,
    pending_pcm: Vec<i16>,
    sample_rate: u32,
    first_pts: Option<i64>,
    encoded_frames: u64,
}

impl Eac3AacTranscoder {
    fn new(sample_rate: u32, initial_pts: Option<i64>) -> Result<Self, String> {
        let mut params = CodecParameters::audio(CodecId::new("eac3"));
        params.sample_rate = Some(sample_rate);
        params.channels = Some(2);
        params.sample_format = Some(SampleFormat::F32);
        let decoder = make_eac3_decoder(&params).map_err(media_error)?;
        let encoder = AacStreamEncoder::new(AacEncoderConfig {
            sample_rate,
            channels: 2,
            bitrate: 128_000,
        })
        .map_err(media_error)?;
        Ok(Self {
            decoder,
            encoder,
            pending_pcm: Vec::with_capacity(1024 * 2),
            sample_rate,
            first_pts: initial_pts,
            encoded_frames: 0,
        })
    }

    fn push(&mut self, data: &[u8], pts: i64) -> Result<Vec<Sample>, String> {
        const CHANNELS: usize = 2;
        const HOP_SAMPLES: usize = 1024 * CHANNELS;

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
                    if pcm.len() != audio.samples as usize * CHANNELS * 4 {
                        return Err("E-AC-3 decoder returned an unexpected PCM format".to_owned());
                    }
                    self.first_pts.get_or_insert(audio.pts.unwrap_or(pts));
                    self.pending_pcm.extend(pcm.chunks_exact(4).map(|bytes| {
                        (f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) * 32767.0)
                            .round()
                            .clamp(i16::MIN as f32, i16::MAX as f32) as i16
                    }));
                    while self.pending_pcm.len() >= HOP_SAMPLES {
                        let hop = self.pending_pcm.drain(..HOP_SAMPLES).collect::<Vec<_>>();
                        if let Some(sample) = self.encode_hop(&hop)? {
                            samples.push(sample);
                        }
                    }
                }
                Ok(_) => return Err("E-AC-3 decoder returned a non-audio frame".to_owned()),
                Err(oxideav_core::Error::NeedMore) => break,
                Err(error) => return Err(media_error(error)),
            }
        }
        Ok(samples)
    }

    fn finish(&mut self) -> Result<Vec<Sample>, String> {
        let mut samples = Vec::new();
        if !self.pending_pcm.is_empty() {
            let tail = std::mem::take(&mut self.pending_pcm);
            if let Some(sample) = self.encode_hop(&tail)? {
                samples.push(sample);
            }
        }
        let frame = self.encoder.finish().map_err(media_error)?;
        if let Some(sample) = self.aac_sample(frame)? {
            samples.push(sample);
        }
        Ok(samples)
    }

    fn encode_hop(&mut self, pcm: &[i16]) -> Result<Option<Sample>, String> {
        let frame = self.encoder.encode_frame(pcm).map_err(media_error)?;
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
            audio_codec: Some("mp4a.40.2".to_owned()),
            width: 1280,
            height: 720,
            audio_segments: Vec::new(),
            audio_timings: Vec::new(),
            audio_sample_rate: Some(48_000),
            audio_channels: Some(2),
            separate_dash_tracks: false,
            transcode_audio: false,
            video_timings: Vec::new(),
            video_transcode: None,
            video_start_segment: 0,
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
            audio_codec: Some("mp4a.40.2".to_owned()),
            width: 1280,
            height: 720,
            transcode_audio: true,
            video_timings: Vec::new(),
            audio_timings: Vec::new(),
            audio_sample_rate: Some(48_000),
            audio_channels: Some(2),
            separate_dash_tracks: true,
            video_transcode: None,
            video_start_segment: 1,
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
            audio_codec: Some("mp4a.40.2".to_owned()),
            width: 1280,
            height: 720,
            audio_segments: Vec::new(),
            audio_timings: Vec::new(),
            audio_sample_rate: Some(48_000),
            audio_channels: Some(2),
            separate_dash_tracks: false,
            transcode_audio: false,
            video_timings: Vec::new(),
            video_transcode: None,
            video_start_segment: 0,
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

        let mut encoder = AacStreamEncoder::new(AacEncoderConfig {
            sample_rate,
            channels: 2,
            bitrate: 128_000,
        })
        .unwrap();
        let pcm = vec![0_i16; 1024 * 2];
        let frames = [
            encoder.encode_frame(&pcm).unwrap(),
            encoder.encode_frame(&pcm).unwrap(),
            encoder.finish().unwrap(),
        ];
        for frame in frames {
            assert_eq!(frame[0], 0xff);
            assert_eq!(frame[1] & 0xf6, 0xf0);
            assert!(frame.len() > 7);
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
}
