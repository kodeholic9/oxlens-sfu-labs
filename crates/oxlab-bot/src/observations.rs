// author: kodeholic (powered by Claude)
//! BotObservations — Layer 1 체크포인트용 봇 관측 데이터
//!
//! 각 봇이 미디어 송수신 중 관측한 사실을 기록.
//! checkpoint_eval이 이 데이터를 기반으로 L1 판정을 수행한다.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use serde::{Serialize, Deserialize};

/// 봇 관측 데이터 (스레드 안전)
#[derive(Clone)]
pub struct BotObservations {
    inner: Arc<Mutex<ObservationsInner>>,
}

/// 관측 데이터 내부
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ObservationsInner {
    // ── L1-01: subscriber RR relay blocked ──
    /// publisher가 수신한 RR의 sender SSRC 목록 (서버만 보내야 함 → 1개)
    pub rr_sender_ssrcs: HashSet<u32>,
    /// publisher가 수신한 RR 총 개수
    pub rr_received_count: u64,

    // ── L1-02, L1-03: SR Translation ──
    /// subscriber가 수신한 SR 목록 [(ssrc, ntp_ts, rtp_ts)]
    pub sr_received: Vec<SrRecord>,

    // ── L1-04: PTT non-speaker gating ──
    /// PTT 비발화 구간에 수신한 RTP 패킷 수 (0이어야 함)
    pub ptt_non_speaker_rtp_count: u64,

    // ── L1-05: PTT silence flush ──
    /// floor release 직후 수신한 Opus silence 프레임 수 (3이어야 함)
    pub ptt_silence_frames_after_release: u64,

    // ── L1-06: PTT speaker switch keyframe ──
    /// 화자 전환 후 첫 번째 video 패킷이 keyframe이었는지
    pub ptt_first_video_was_keyframe: Option<bool>,

    // ── L1-07: SSRC rewriting consistency ──
    /// subscriber가 수신한 audio virtual SSRC 목록 (PTT: 1개여야 함)
    pub ptt_audio_ssrcs: HashSet<u32>,
    /// subscriber가 수신한 video virtual SSRC 목록 (PTT: 1개여야 함)
    pub ptt_video_ssrcs: HashSet<u32>,

    // ── L1-09, L1-10: PLI Governor ──
    /// publisher가 수신한 PLI 총 개수
    pub pli_received_count: u64,
    /// publisher가 PLI 수신 → keyframe 응답까지 시간 (ms) 기록
    pub pli_to_keyframe_ms: Vec<u64>,

    // ── L1-11, L1-12: SubscriberGate ──
    /// TRACKS_ACK 전에 수신한 video 패킷 수 (0이어야 함)
    pub video_before_ack_count: u64,
    /// TRACKS_ACK 완료 여부
    pub tracks_ack_sent: bool,

    // ── L1-14 ~ L1-16: Floor Control ──
    /// floor_request 시 부여받은 grant 순서 [(bot_id, priority, grant_order)]
    pub floor_grants: Vec<FloorGrantRecord>,
    /// floor revoke 수신 여부
    pub floor_revoke_received: bool,
    /// queue position 응답 기록
    pub queue_positions: Vec<u32>,

    // ── L1-17: Lifecycle ──
    /// 좀비 봇 감지 (ROOM_EVENT leave 수신한 user_id 목록)
    pub zombie_cleanup_detected: Vec<String>,

    // ── L1-08: PTT ts_gap ──
    /// idle 후 복귀 시 RTP ts gap 기록
    pub ptt_ts_gap_records: Vec<TsGapRecord>,

    // ── L1-18 ~ L1-20: Simulcast ──
    /// 레이어 전환 이벤트 기록
    pub simulcast_layer_switches: Vec<LayerSwitchEvent>,

    // ── L1-21: Screen Share ──
    /// publisher가 송신한 screen share SSRC (publisher 봇 기록)
    pub screen_share_ssrc_published: Option<u32>,
    /// subscriber가 수신한 screen share SSRC 목록
    pub screen_share_ssrcs_received: HashSet<u32>,
}

/// SR 수신 기록
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrRecord {
    pub ssrc: u32,
    pub ntp_sec: u32,
    pub ntp_frac: u32,
    pub rtp_ts: u32,
    pub packet_count: u32,
    pub octet_count: u32,
}

/// Floor grant 기록
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FloorGrantRecord {
    pub bot_id: String,
    pub priority: u8,
    pub granted: bool,
    pub queued: bool,
    pub timestamp_ms: u64,
}

/// PTT idle 후 복귀 시 ts gap 기록
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TsGapRecord {
    pub ssrc: u32,
    /// 실제 벽시계 gap (ms)
    pub wall_gap_ms: u64,
    /// RTP timestamp gap
    pub rtp_ts_gap: u32,
    /// 샘플레이트 (audio=48000, video=90000)
    pub sample_rate: u32,
}

/// Simulcast 레이어 전환 이벤트
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerSwitchEvent {
    /// 요청한 레이어 ("h", "l", "pause")
    pub requested_layer: String,
    /// SUBSCRIBE_LAYER 전송 시점 (ms, epoch)
    pub request_timestamp_ms: u64,
    /// 전환 후 첫 keyframe 도착까지 시간 (ms), 미도착 시 None
    pub keyframe_after_ms: Option<u64>,
    /// 전환 전 마지막 RTP ts
    pub ts_before: Option<u32>,
    /// 전환 후 첫 RTP ts
    pub ts_after: Option<u32>,
    /// 전환 후 패킷 수신 여부 (타임아웃 내 패킷 도착 여부)
    pub packets_after: bool,
}

impl BotObservations {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ObservationsInner::default())),
        }
    }

    // ── RR 관측 (L1-01) ──

    pub fn record_rr(&self, sender_ssrc: u32) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.rr_sender_ssrcs.insert(sender_ssrc);
            obs.rr_received_count += 1;
        }
    }

    // ── SR 관측 (L1-02, L1-03) ──

    pub fn record_sr(&self, ssrc: u32, ntp_sec: u32, ntp_frac: u32, rtp_ts: u32, pkt_count: u32, oct_count: u32) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.sr_received.push(SrRecord {
                ssrc, ntp_sec, ntp_frac, rtp_ts,
                packet_count: pkt_count, octet_count: oct_count,
            });
        }
    }

    // ── PTT 관측 (L1-04 ~ L1-08) ──

    pub fn record_ptt_non_speaker_rtp(&self) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.ptt_non_speaker_rtp_count += 1;
        }
    }

    pub fn record_ptt_silence_frame(&self) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.ptt_silence_frames_after_release += 1;
        }
    }

    pub fn record_ptt_first_video_keyframe(&self, is_keyframe: bool) {
        if let Ok(mut obs) = self.inner.lock() {
            if obs.ptt_first_video_was_keyframe.is_none() {
                obs.ptt_first_video_was_keyframe = Some(is_keyframe);
            }
        }
    }

    pub fn record_ptt_audio_ssrc(&self, ssrc: u32) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.ptt_audio_ssrcs.insert(ssrc);
        }
    }

    pub fn record_ptt_video_ssrc(&self, ssrc: u32) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.ptt_video_ssrcs.insert(ssrc);
        }
    }

    // ── PLI 관측 (L1-09, L1-10) ──

    pub fn record_pli(&self) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.pli_received_count += 1;
        }
    }

    pub fn record_pli_to_keyframe(&self, elapsed_ms: u64) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.pli_to_keyframe_ms.push(elapsed_ms);
        }
    }

    // ── SubscriberGate 관측 (L1-11, L1-12) ──

    pub fn record_video_before_ack(&self) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.video_before_ack_count += 1;
        }
    }

    pub fn mark_tracks_ack_sent(&self) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.tracks_ack_sent = true;
        }
    }

    // ── Floor Control 관측 (L1-14 ~ L1-16) ──

    pub fn record_floor_grant(&self, bot_id: &str, priority: u8, granted: bool, queued: bool) {
        if let Ok(mut obs) = self.inner.lock() {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            obs.floor_grants.push(FloorGrantRecord {
                bot_id: bot_id.to_string(),
                priority,
                granted,
                queued,
                timestamp_ms: ts,
            });
        }
    }

    pub fn record_floor_revoke(&self) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.floor_revoke_received = true;
        }
    }

    pub fn record_queue_position(&self, pos: u32) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.queue_positions.push(pos);
        }
    }

    // ── Lifecycle 관측 (L1-17) ──

    pub fn record_zombie_cleanup(&self, user_id: &str) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.zombie_cleanup_detected.push(user_id.to_string());
        }
    }

    // ── PTT ts_gap 관측 (L1-08) ──

    pub fn record_ts_gap(&self, ssrc: u32, wall_gap_ms: u64, rtp_ts_gap: u32, sample_rate: u32) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.ptt_ts_gap_records.push(TsGapRecord {
                ssrc, wall_gap_ms, rtp_ts_gap, sample_rate,
            });
        }
    }

    // ── Simulcast 관측 (L1-18 ~ L1-20) ──

    pub fn record_layer_switch(&self, event: LayerSwitchEvent) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.simulcast_layer_switches.push(event);
        }
    }

    // ── Screen Share 관측 (L1-21) ──

    pub fn record_screen_share_published(&self, ssrc: u32) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.screen_share_ssrc_published = Some(ssrc);
        }
    }

    pub fn record_screen_share_received(&self, ssrc: u32) {
        if let Ok(mut obs) = self.inner.lock() {
            obs.screen_share_ssrcs_received.insert(ssrc);
        }
    }

    // ── 스냅샷 ──

    pub fn snapshot(&self) -> ObservationsInner {
        self.inner.lock().map(|o| o.clone()).unwrap_or_default()
    }
}
