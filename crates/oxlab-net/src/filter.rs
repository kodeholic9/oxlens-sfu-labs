// author: kodeholic (powered by Claude)
//! NetFilter — 유저스페이스 패킷 레벨 열화 주입
//!
//! Phase 0: drop (균일 확률) + delay (고정 + jitter) + bandwidth (토큰 버킷)
//! Phase 1 예정: Gilbert-Elliott burst, reorder, corrupt, duplicate

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tracing::trace;

// ── 설정 ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterConfig {
    /// 패킷 드롭 확률 (0.0 ~ 100.0)
    #[serde(default)]
    pub loss_percent: f64,

    /// 고정 지연 (ms)
    #[serde(default)]
    pub delay_ms: u32,

    /// 지연 jitter (ms, 정규분포 근사 — uniform ±jitter)
    #[serde(default)]
    pub jitter_ms: u32,

    /// 대역폭 제한 (kbps, 0 = 무제한)
    #[serde(default)]
    pub bandwidth_kbps: u32,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            loss_percent: 0.0,
            delay_ms: 0,
            jitter_ms: 0,
            bandwidth_kbps: 0,
        }
    }
}

// ── 필터 결과 ──

#[derive(Debug, Clone)]
pub enum FilterResult {
    /// 패킷 드롭
    Drop,
    /// 패킷 통과 — delay 후 전송
    Pass { delay: Duration },
}

// ── 토큰 버킷 (대역폭 제한) ──

struct TokenBucket {
    tokens: f64,        // 현재 토큰 (bytes)
    capacity: f64,      // 최대 토큰 (bytes) — burst 허용량
    rate: f64,          // 초당 충전 bytes
    last_refill: Instant,
}

impl TokenBucket {
    fn new(bandwidth_kbps: u32) -> Self {
        let rate = (bandwidth_kbps as f64) * 1000.0 / 8.0; // kbps → bytes/sec
        let capacity = rate * 0.1; // 100ms 분량 burst 허용
        Self {
            tokens: capacity, // 시작 시 full
            capacity,
            rate,
            last_refill: Instant::now(),
        }
    }

    /// 토큰 충전 + 소비 시도. 통과 가능하면 true.
    fn consume(&mut self, bytes: usize) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;

        // 충전
        self.tokens = (self.tokens + self.rate * elapsed).min(self.capacity);

        // 소비
        let cost = bytes as f64;
        if self.tokens >= cost {
            self.tokens -= cost;
            true
        } else {
            false // 대역폭 초과 → 드롭
        }
    }
}

// ── NetFilter 본체 ──

pub struct NetFilter {
    config: FilterConfig,
    rng: SmallRng,
    token_bucket: Option<TokenBucket>,
    /// 통계: 총 패킷 수
    pub stats_total: u64,
    /// 통계: 드롭 패킷 수
    pub stats_dropped: u64,
}

impl NetFilter {
    pub fn new(config: FilterConfig) -> Self {
        let token_bucket = if config.bandwidth_kbps > 0 {
            Some(TokenBucket::new(config.bandwidth_kbps))
        } else {
            None
        };

        Self {
            config,
            rng: SmallRng::from_entropy(),
            token_bucket,
            stats_total: 0,
            stats_dropped: 0,
        }
    }

    /// "통과 필터" — pristine 환경 (열화 없음)
    pub fn pristine() -> Self {
        Self::new(FilterConfig::default())
    }

    /// 패킷 필터링. 봇의 RTP 송수신 시 호출.
    pub fn filter(&mut self, packet_len: usize) -> FilterResult {
        self.stats_total += 1;

        // 1) 대역폭 제한
        if let Some(ref mut bucket) = self.token_bucket {
            if !bucket.consume(packet_len) {
                self.stats_dropped += 1;
                trace!("netfilter: bw drop (pkt={}B)", packet_len);
                return FilterResult::Drop;
            }
        }

        // 2) 랜덤 드롭
        if self.config.loss_percent > 0.0 {
            let roll: f64 = self.rng.gen_range(0.0..100.0);
            if roll < self.config.loss_percent {
                self.stats_dropped += 1;
                trace!("netfilter: loss drop (roll={:.1}%)", roll);
                return FilterResult::Drop;
            }
        }

        // 3) 지연 계산
        let base_delay = self.config.delay_ms as i64;
        let jitter = if self.config.jitter_ms > 0 {
            let j = self.config.jitter_ms as i64;
            self.rng.gen_range(-j..=j)
        } else {
            0
        };
        let total_ms = (base_delay + jitter).max(0) as u64;

        FilterResult::Pass {
            delay: Duration::from_millis(total_ms),
        }
    }

    /// 런타임 설정 동적 전환 (시나리오 엔진이 호출)
    pub fn update(&mut self, config: FilterConfig) {
        self.token_bucket = if config.bandwidth_kbps > 0 {
            Some(TokenBucket::new(config.bandwidth_kbps))
        } else {
            None
        };
        self.config = config;
    }

    /// 현재 설정 참조
    pub fn config(&self) -> &FilterConfig {
        &self.config
    }

    /// 드롭률 통계 (percent)
    pub fn drop_rate(&self) -> f64 {
        if self.stats_total == 0 {
            return 0.0;
        }
        (self.stats_dropped as f64 / self.stats_total as f64) * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pristine_never_drops() {
        let mut f = NetFilter::pristine();
        for _ in 0..10000 {
            match f.filter(1200) {
                FilterResult::Drop => panic!("pristine should never drop"),
                FilterResult::Pass { delay } => {
                    assert_eq!(delay, Duration::ZERO);
                }
            }
        }
    }

    #[test]
    fn full_loss_always_drops() {
        let mut f = NetFilter::new(FilterConfig {
            loss_percent: 100.0,
            ..Default::default()
        });
        for _ in 0..1000 {
            assert!(matches!(f.filter(1200), FilterResult::Drop));
        }
    }

    #[test]
    fn delay_and_jitter() {
        let mut f = NetFilter::new(FilterConfig {
            delay_ms: 50,
            jitter_ms: 10,
            ..Default::default()
        });
        let mut min_ms = u64::MAX;
        let mut max_ms = 0;
        for _ in 0..10000 {
            if let FilterResult::Pass { delay } = f.filter(1200) {
                let ms = delay.as_millis() as u64;
                min_ms = min_ms.min(ms);
                max_ms = max_ms.max(ms);
            }
        }
        // 50 ± 10 → 40~60 범위
        assert!(min_ms >= 40, "min_ms={}", min_ms);
        assert!(max_ms <= 60, "max_ms={}", max_ms);
    }

    #[test]
    fn bandwidth_limit_drops_burst() {
        // 100 kbps = 12500 bytes/sec
        let mut f = NetFilter::new(FilterConfig {
            bandwidth_kbps: 100,
            ..Default::default()
        });
        let mut dropped = 0;
        // 한번에 1200B × 100패킷 = 120KB → 12.5KB 용량 초과해서 드롭 발생해야 함
        for _ in 0..100 {
            if matches!(f.filter(1200), FilterResult::Drop) {
                dropped += 1;
            }
        }
        assert!(dropped > 0, "bandwidth limit should cause drops");
    }

    #[test]
    fn dynamic_update() {
        let mut f = NetFilter::pristine();
        // pristine → 100% loss
        f.update(FilterConfig {
            loss_percent: 100.0,
            ..Default::default()
        });
        assert!(matches!(f.filter(100), FilterResult::Drop));
    }
}
