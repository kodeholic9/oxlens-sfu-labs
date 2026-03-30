// author: kodeholic (powered by Claude)
//! RtpSubscriber — 봇용 RTP 수신 메트릭 수집기
//!
//! sub_socket에서 서버가 fan-out한 SRTP를 수신 → decrypt → SSRC별 메트릭 수집.
//! RFC 3550 interarrival jitter, seq gap 기반 loss, out-of-order 감지.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

// ── SSRC별 수신 메트릭 ──

/// 단일 SSRC의 수신 메트릭 (RFC 3550 기반)
#[derive(Debug, Clone)]
pub struct SsrcRecvMetrics {
    pub ssrc: u32,
    pub pt: u8,
    pub packets_received: u64,
    pub packets_lost: u64,
    pub out_of_order: u64,
    /// RFC 3550 interarrival jitter (RTP timestamp 단위)
    pub jitter: f64,
    /// 마지막 수신 seq
    last_seq: u16,
    /// 최고 seq (wrap 고려)
    max_seq_seen: u16,
    /// 첫 수신 seq
    pub first_seq: u16,
    /// 첫 패킷 여부
    initialized: bool,
    /// 이전 패킷 도착 시각 (jitter 계산용)
    last_arrival: Instant,
    /// 이전 RTP timestamp (jitter 계산용)
    last_rtp_ts: u32,
    /// 예상 clock rate (audio=48000, video=90000 — PT 기반 추정)
    clock_rate: u32,
}

impl SsrcRecvMetrics {
    fn new(ssrc: u32, pt: u8) -> Self {
        // PT 기반 clock rate 추정
        let clock_rate = if pt == 111 { 48_000 } else { 90_000 };
        Self {
            ssrc,
            pt,
            packets_received: 0,
            packets_lost: 0,
            out_of_order: 0,
            jitter: 0.0,
            last_seq: 0,
            max_seq_seen: 0,
            first_seq: 0,
            initialized: false,
            last_arrival: Instant::now(),
            last_rtp_ts: 0,
            clock_rate,
        }
    }

    fn update(&mut self, seq: u16, ts: u32) {
        self.packets_received += 1;
        let now = Instant::now();

        if !self.initialized {
            self.first_seq = seq;
            self.last_seq = seq;
            self.max_seq_seen = seq;
            self.last_arrival = now;
            self.last_rtp_ts = ts;
            self.initialized = true;
            return;
        }

        // seq gap → loss/out-of-order 판별
        let expected = self.last_seq.wrapping_add(1);
        let diff = seq.wrapping_sub(expected) as i16;

        if diff > 0 {
            // gap: diff 만큼 loss
            self.packets_lost += diff as u64;
        } else if diff < -1 {
            // 과거 seq → out of order (1 차이는 정상 순서)
            self.out_of_order += 1;
        }

        // max_seq 갱신 (forward 방향만)
        let fwd = seq.wrapping_sub(self.max_seq_seen);
        if (fwd as i16) > 0 {
            self.max_seq_seen = seq;
        }

        // RFC 3550 jitter 계산
        // J(i) = J(i-1) + (|D(i-1,i)| - J(i-1)) / 16
        // D(i-1,i) = (arrival_diff_in_rtp_units) - (rtp_ts_diff)
        let arrival_diff_ms = now.duration_since(self.last_arrival).as_secs_f64() * 1000.0;
        let arrival_diff_ticks = arrival_diff_ms * (self.clock_rate as f64) / 1000.0;
        let rtp_diff = ts.wrapping_sub(self.last_rtp_ts) as f64;
        let d = (arrival_diff_ticks - rtp_diff).abs();
        self.jitter += (d - self.jitter) / 16.0;

        self.last_seq = seq;
        self.last_arrival = now;
        self.last_rtp_ts = ts;
    }
}

// ── 전체 수신 메트릭 (SSRC 집계) ──

/// 모든 SSRC의 수신 메트릭 집계
#[derive(Debug, Clone, Default)]
pub struct RecvSnapshot {
    pub total_packets: u64,
    pub total_lost: u64,
    pub total_ooo: u64,
    pub ssrc_count: usize,
    pub streams: Vec<SsrcRecvMetrics>,
}

/// 스레드 안전 메트릭 저장소
#[derive(Clone)]
pub struct RecvMetricsStore {
    inner: Arc<Mutex<HashMap<u32, SsrcRecvMetrics>>>,
}

impl RecvMetricsStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// RTP 패킷 수신 시 메트릭 업데이트
    pub fn update(&self, ssrc: u32, pt: u8, seq: u16, ts: u32) {
        if let Ok(mut map) = self.inner.lock() {
            let entry = map
                .entry(ssrc)
                .or_insert_with(|| SsrcRecvMetrics::new(ssrc, pt));
            entry.update(seq, ts);
        }
    }

    /// 현재 메트릭 스냅샷
    pub fn snapshot(&self) -> RecvSnapshot {
        let map = match self.inner.lock() {
            Ok(m) => m,
            Err(_) => return RecvSnapshot::default(),
        };

        let streams: Vec<SsrcRecvMetrics> = map.values().cloned().collect();
        let total_packets = streams.iter().map(|s| s.packets_received).sum();
        let total_lost = streams.iter().map(|s| s.packets_lost).sum();
        let total_ooo = streams.iter().map(|s| s.out_of_order).sum();

        RecvSnapshot {
            total_packets,
            total_lost,
            total_ooo,
            ssrc_count: streams.len(),
            streams,
        }
    }
}

impl SsrcRecvMetrics {
    /// 테스트용 생성자 (rx/lost 직접 설정)
    #[cfg(any(test, feature = "test-util"))]
    pub fn new_for_test(ssrc: u32, pt: u8, packets_received: u64, packets_lost: u64) -> Self {
        let mut m = Self::new(ssrc, pt);
        m.packets_received = packets_received;
        m.packets_lost = packets_lost;
        m
    }
}

/// 스냅샷을 로그 한 줄로 포맷
pub fn format_recv_summary(bot_id: &str, snap: &RecvSnapshot) -> String {
    if snap.ssrc_count == 0 {
        return format!("[bot:{}] recv: no streams", bot_id);
    }

    let details: Vec<String> = snap.streams.iter().map(|s| {
        let kind = if s.pt == 111 { "A" } else { "V" };
        format!(
            "{}:0x{:08X}(rx={} lost={} ooo={} jit={:.0})",
            kind, s.ssrc, s.packets_received, s.packets_lost, s.out_of_order, s.jitter
        )
    }).collect();

    format!(
        "[bot:{}] recv: pkts={} lost={} ooo={} streams={} [{}]",
        bot_id, snap.total_packets, snap.total_lost, snap.total_ooo,
        snap.ssrc_count, details.join(", ")
    )
}

// ── Tests ──

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_rtp_mini() {
        let mut pkt = vec![0u8; 20];
        pkt[0] = 0x80; // V=2
        pkt[1] = 111;  // PT=111
        pkt[2..4].copy_from_slice(&42u16.to_be_bytes());
        pkt[4..8].copy_from_slice(&960u32.to_be_bytes());
        pkt[8..12].copy_from_slice(&0xDEADBEEFu32.to_be_bytes());

        let rtp = parse_rtp_mini(&pkt).unwrap();
        assert_eq!(rtp.pt, 111);
        assert_eq!(rtp.seq, 42);
        assert_eq!(rtp.ts, 960);
        assert_eq!(rtp.ssrc, 0xDEADBEEF);
        assert!(!rtp.marker);
    }

    #[test]
    fn test_parse_rtp_marker() {
        let mut pkt = vec![0u8; 12];
        pkt[0] = 0x80;
        pkt[1] = 96 | 0x80; // PT=96 + marker
        let rtp = parse_rtp_mini(&pkt).unwrap();
        assert_eq!(rtp.pt, 96);
        assert!(rtp.marker);
    }

    #[test]
    fn test_ssrc_metrics_basic() {
        let mut m = SsrcRecvMetrics::new(1000, 111);

        // seq 0, 1, 2 → no loss
        m.update(0, 0);
        m.update(1, 960);
        m.update(2, 1920);
        assert_eq!(m.packets_received, 3);
        assert_eq!(m.packets_lost, 0);
        assert_eq!(m.out_of_order, 0);
    }

    #[test]
    fn test_ssrc_metrics_loss() {
        let mut m = SsrcRecvMetrics::new(1000, 111);

        m.update(0, 0);
        // skip seq 1, 2 → gap of 2
        m.update(3, 2880);
        assert_eq!(m.packets_received, 2);
        assert_eq!(m.packets_lost, 2);
    }

    #[test]
    fn test_ssrc_metrics_ooo() {
        let mut m = SsrcRecvMetrics::new(1000, 111);

        m.update(0, 0);
        m.update(1, 960);
        m.update(3, 2880); // skip 2 → lost=1
        m.update(2, 1920); // late arrival → ooo
        assert_eq!(m.packets_lost, 1);
        assert_eq!(m.out_of_order, 1);
    }

    #[test]
    fn test_recv_metrics_store() {
        let store = RecvMetricsStore::new();
        store.update(1000, 111, 0, 0);
        store.update(1000, 111, 1, 960);
        store.update(2000, 96, 0, 0);

        let snap = store.snapshot();
        assert_eq!(snap.ssrc_count, 2);
        assert_eq!(snap.total_packets, 3);
    }

    #[test]
    fn test_format_recv_summary() {
        let snap = RecvSnapshot {
            total_packets: 100,
            total_lost: 2,
            total_ooo: 1,
            ssrc_count: 2,
            streams: vec![
                SsrcRecvMetrics::new(0xA000, 111),
                SsrcRecvMetrics::new(0xB000, 96),
            ],
        };
        let s = format_recv_summary("bot_1", &snap);
        assert!(s.contains("bot_1"));
        assert!(s.contains("pkts=100"));
        assert!(s.contains("lost=2"));
    }
}
