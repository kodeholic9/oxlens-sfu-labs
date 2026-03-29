// author: kodeholic (powered by Claude)
//! FakeRtpPublisher — 봇용 가짜 RTP 패킷 생성기
//!
//! Audio: Opus silence (PT=111, 20ms 간격, 960 samples/frame)
//! Video: VP8 minimal (configurable PT, 33ms 간격 ≈ 30fps, 2초 keyframe)
//!
//! Phase D 호환: MID RTP header extension 포함 → 서버 stream discovery 정상 동작.
//! One-Byte Header Extension (RFC 5285) 사용.

// ── 상수 ──

/// Opus payload type (전 브라우저 고정)
pub const OPUS_PT: u8 = 111;
/// Opus clock rate
const OPUS_CLOCK_RATE: u32 = 48_000;
/// Opus frame duration 20ms → 960 samples
const OPUS_SAMPLES_PER_FRAME: u32 = 960;
/// Opus silence payload (DTX compatible, 3 bytes)
const OPUS_SILENCE: [u8; 3] = [0xF8, 0xFF, 0xFE];

/// VP8 default PT
pub const VP8_DEFAULT_PT: u8 = 96;
/// VP8 clock rate
const VP8_CLOCK_RATE: u32 = 90_000;
/// VP8 frame duration ~33ms → 3000 ticks at 90kHz
const VP8_TICKS_PER_FRAME: u32 = 3_000;
/// Keyframe interval (frames)
const VP8_KEYFRAME_INTERVAL: u32 = 60; // 2초 at 30fps
/// VP8 fake payload size (keyframe larger than P-frame)
const VP8_KEYFRAME_SIZE: usize = 800;
const VP8_PFRAME_SIZE: usize = 200;

/// RFC 5285 One-Byte Header profile
const ONE_BYTE_HEADER_PROFILE: u16 = 0xBEDE;

// ── RTP 패킷 생성 설정 ──

#[derive(Debug, Clone)]
pub struct RtpPublisherConfig {
    pub audio_ssrc: u32,
    pub video_ssrc: u32,
    pub video_pt: u8,
    pub mid_extmap_id: u8,
    pub audio_mid: String,
    pub video_mid: String,
}

impl Default for RtpPublisherConfig {
    fn default() -> Self {
        Self {
            audio_ssrc: 0xA000_0001,
            video_ssrc: 0xB000_0001,
            video_pt: VP8_DEFAULT_PT,
            mid_extmap_id: 1,
            audio_mid: "0".to_string(),
            video_mid: "1".to_string(),
        }
    }
}

// ── 패킷 빌더 상태 ──

pub struct RtpPublisherState {
    pub config: RtpPublisherConfig,
    pub audio_seq: u16,
    pub video_seq: u16,
    pub audio_ts: u32,
    pub video_ts: u32,
    pub video_frame_count: u32,
    /// 외부에서 설정 — 다음 프레임을 키프레임으로 강제 (PLI 응답)
    pub force_keyframe: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl RtpPublisherState {
    pub fn new(config: RtpPublisherConfig) -> Self {
        Self {
            config,
            audio_seq: 0,
            video_seq: 0,
            audio_ts: 0,
            video_ts: 0,
            video_frame_count: 0,
            force_keyframe: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn new_with_keyframe_flag(
        config: RtpPublisherConfig,
        flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            config,
            audio_seq: 0,
            video_seq: 0,
            audio_ts: 0,
            video_ts: 0,
            video_frame_count: 0,
            force_keyframe: flag,
        }
    }

    /// Opus silence RTP 패킷 생성 (20ms 간격 호출)
    pub fn build_opus_packet(&mut self) -> Vec<u8> {
        let mid = &self.config.audio_mid;
        let ext_size = mid_extension_words(mid);
        let header_len = 12 + 4 + ext_size * 4; // RTP fixed + ext profile+len + ext data
        let total = header_len + OPUS_SILENCE.len();

        let mut pkt = vec![0u8; total];

        // RTP fixed header (12 bytes)
        pkt[0] = 0x90; // V=2, X=1 (extension present)
        pkt[1] = OPUS_PT;
        pkt[2..4].copy_from_slice(&self.audio_seq.to_be_bytes());
        pkt[4..8].copy_from_slice(&self.audio_ts.to_be_bytes());
        pkt[8..12].copy_from_slice(&self.config.audio_ssrc.to_be_bytes());

        // Header extension
        write_mid_extension(&mut pkt, 12, self.config.mid_extmap_id, mid);

        // Payload
        pkt[header_len..].copy_from_slice(&OPUS_SILENCE);

        // Advance state
        self.audio_seq = self.audio_seq.wrapping_add(1);
        self.audio_ts = self.audio_ts.wrapping_add(OPUS_SAMPLES_PER_FRAME);

        pkt
    }

    /// VP8 RTP 패킷 생성 (33ms 간격 호출)
    pub fn build_vp8_packet(&mut self) -> Vec<u8> {
        let forced = self.force_keyframe.swap(false, std::sync::atomic::Ordering::Relaxed);
        let is_keyframe = forced || self.video_frame_count % VP8_KEYFRAME_INTERVAL == 0;
        let payload_size = if is_keyframe { VP8_KEYFRAME_SIZE } else { VP8_PFRAME_SIZE };
        let mid = &self.config.video_mid;
        let ext_size = mid_extension_words(mid);
        let header_len = 12 + 4 + ext_size * 4;
        // VP8 payload descriptor (1 byte) + fake payload
        let total = header_len + 1 + payload_size;

        let mut pkt = vec![0u8; total];

        // RTP fixed header
        pkt[0] = 0x90; // V=2, X=1
        pkt[1] = self.config.video_pt;
        if is_keyframe {
            pkt[1] |= 0x80; // Marker bit (frame boundary)
        }
        pkt[2..4].copy_from_slice(&self.video_seq.to_be_bytes());
        pkt[4..8].copy_from_slice(&self.video_ts.to_be_bytes());
        pkt[8..12].copy_from_slice(&self.config.video_ssrc.to_be_bytes());

        // Header extension
        write_mid_extension(&mut pkt, 12, self.config.mid_extmap_id, mid);

        // VP8 payload descriptor (minimal, 1 byte): S=1 (start of partition), PID=0
        let pd_offset = header_len;
        pkt[pd_offset] = 0x10;

        // VP8 fake payload
        if payload_size > 0 {
            let data_offset = pd_offset + 1;
            if is_keyframe {
                // VP8 keyframe: frame_type=0, show_frame=1
                pkt[data_offset] = 0x10;
            } else {
                // VP8 interframe: frame_type=1, show_frame=1
                pkt[data_offset] = 0x11;
            }
        }

        // Advance state
        self.video_seq = self.video_seq.wrapping_add(1);
        self.video_ts = self.video_ts.wrapping_add(VP8_TICKS_PER_FRAME);
        self.video_frame_count += 1;

        pkt
    }
}

// ── RTP Header Extension 헬퍼 (RFC 5285 One-Byte) ──

/// MID extension이 차지하는 32-bit word 수 계산
fn mid_extension_words(mid: &str) -> usize {
    // One-Byte element: 1 byte header (ID:4 + L:4) + L+1 bytes data
    let element_len = 1 + mid.len();
    (element_len + 3) / 4 // ceil to 32-bit words
}

/// RTP 패킷에 One-Byte Header Extension (MID only) 작성
///
/// offset: extension 시작 위치 (= RTP fixed header 끝, CSRC 없으면 12)
fn write_mid_extension(pkt: &mut [u8], offset: usize, mid_extmap_id: u8, mid: &str) {
    let ext_words = mid_extension_words(mid);

    // Extension profile (2 bytes)
    pkt[offset] = (ONE_BYTE_HEADER_PROFILE >> 8) as u8;
    pkt[offset + 1] = ONE_BYTE_HEADER_PROFILE as u8;

    // Extension length in 32-bit words (2 bytes)
    pkt[offset + 2] = (ext_words >> 8) as u8;
    pkt[offset + 3] = ext_words as u8;

    // One-Byte element: ID (4 bits) | L (4 bits, = data_len - 1) | data
    let data_offset = offset + 4;
    let data_len = mid.len();
    pkt[data_offset] = (mid_extmap_id << 4) | ((data_len - 1) as u8 & 0x0F);

    // MID value (ASCII)
    pkt[data_offset + 1..data_offset + 1 + data_len].copy_from_slice(mid.as_bytes());
    // Remaining bytes already zero (padding)
}

// ── PUBLISH_TRACKS Intent JSON 생성 ──

/// Phase D PUBLISH_TRACKS intent payload 생성
pub fn build_publish_intent_json(config: &RtpPublisherConfig) -> serde_json::Value {
    serde_json::json!({
        "tracks": [
            { "kind": "audio", "ssrc": 0 },
            { "kind": "video", "ssrc": 0, "source": "camera", "codec": "VP8" }
        ],
        "mid_extmap_id": config.mid_extmap_id,
        "audio_mid": config.audio_mid,
        "video_mid": config.video_mid,
        "video_pt": config.video_pt,
        "rtx_pt": config.video_pt + 1
    })
}

// ── Tests ──

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_opus_packet_structure() {
        let config = RtpPublisherConfig {
            audio_ssrc: 0xA0000001,
            video_ssrc: 0xB0000001,
            video_pt: VP8_DEFAULT_PT,
            mid_extmap_id: 1,
            audio_mid: "0".to_string(),
            video_mid: "1".to_string(),
        };
        let mut state = RtpPublisherState::new(config);
        let pkt = state.build_opus_packet();

        // V=2, X=1
        assert_eq!(pkt[0], 0x90);
        // PT=111
        assert_eq!(pkt[1], OPUS_PT);
        // seq=0
        assert_eq!(&pkt[2..4], &[0, 0]);
        // ts=0
        assert_eq!(&pkt[4..8], &[0, 0, 0, 0]);
        // SSRC
        assert_eq!(&pkt[8..12], &0xA0000001u32.to_be_bytes());
        // Extension profile 0xBEDE
        assert_eq!(&pkt[12..14], &[0xBE, 0xDE]);
        // Extension length = 1 word
        assert_eq!(&pkt[14..16], &[0, 1]);
        // MID element: ID=1, L=0 (1 byte data)
        assert_eq!(pkt[16], 0x10);
        // MID value '0' = 0x30
        assert_eq!(pkt[17], b'0');
        // Opus silence at offset 20
        assert_eq!(&pkt[20..23], &OPUS_SILENCE);
        assert_eq!(pkt.len(), 23);

        // Second packet: seq=1, ts=960
        let pkt2 = state.build_opus_packet();
        assert_eq!(&pkt2[2..4], &1u16.to_be_bytes());
        assert_eq!(&pkt2[4..8], &960u32.to_be_bytes());
    }

    #[test]
    fn test_vp8_keyframe_and_pframe() {
        let config = RtpPublisherConfig {
            audio_ssrc: 0xA0000001,
            video_ssrc: 0xB0000001,
            video_pt: 96,
            mid_extmap_id: 1,
            audio_mid: "0".to_string(),
            video_mid: "1".to_string(),
        };
        let mut state = RtpPublisherState::new(config);

        // First frame = keyframe
        let kf = state.build_vp8_packet();
        assert_eq!(kf[0], 0x90);
        assert_eq!(kf[1], 96 | 0x80); // PT + marker
        assert_eq!(&kf[12..14], &[0xBE, 0xDE]);
        assert_eq!(kf[17], b'1'); // MID "1"
        assert_eq!(kf[20], 0x10); // VP8 PD S=1
        assert_eq!(kf[21], 0x10); // keyframe

        // Second frame = P-frame
        let pf = state.build_vp8_packet();
        assert_eq!(pf[1], 96); // no marker
        assert_eq!(pf[21], 0x11); // interframe
    }

    #[test]
    fn test_mid_extension_words() {
        assert_eq!(mid_extension_words("0"), 1);
        assert_eq!(mid_extension_words("1"), 1);
        assert_eq!(mid_extension_words("10"), 1);
        assert_eq!(mid_extension_words("100"), 1);
        assert_eq!(mid_extension_words("1000"), 2);
    }

    #[test]
    fn test_publish_intent_json() {
        let config = RtpPublisherConfig {
            audio_ssrc: 1,
            video_ssrc: 2,
            video_pt: 96,
            mid_extmap_id: 1,
            audio_mid: "0".to_string(),
            video_mid: "1".to_string(),
        };
        let json = build_publish_intent_json(&config);
        assert_eq!(json["mid_extmap_id"], 1);
        assert_eq!(json["audio_mid"], "0");
        assert_eq!(json["video_mid"], "1");
        assert_eq!(json["video_pt"], 96);
        assert_eq!(json["rtx_pt"], 97);
        assert_eq!(json["tracks"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_keyframe_interval() {
        let config = RtpPublisherConfig::default();
        let mut state = RtpPublisherState::new(config);

        // Frame 0 = keyframe (marker bit set)
        let pkt0 = state.build_vp8_packet();
        assert_ne!(pkt0[1] & 0x80, 0, "frame 0 should be keyframe");

        // Frame 1..59 = P-frame
        for _ in 1..60 {
            let pkt = state.build_vp8_packet();
            assert_eq!(pkt[1] & 0x80, 0, "should be P-frame");
        }

        // Frame 60 = keyframe again
        let pkt60 = state.build_vp8_packet();
        assert_ne!(pkt60[1] & 0x80, 0, "frame 60 should be keyframe");
    }
}
