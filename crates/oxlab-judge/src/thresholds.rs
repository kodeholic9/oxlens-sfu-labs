// author: kodeholic (powered by Claude)
//! RegressionThresholds — Layer 2 회귀 감지 임계치
//!
//! 절대 기준이 아닌 "이전 대비 변화량" 임계치.
//! 같은 시나리오+프로파일에서 이전 실행 대비 회귀를 감지한다.

use serde::{Deserialize, Serialize};

/// 회귀 감지 임계치
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionThresholds {
    /// loss rate 증가 허용 (절대%, e.g. 2.0 → 이전 대비 +2%p 이상이면 REGRESS)
    #[serde(default = "default_loss_delta")]
    pub loss_rate_delta: f64,

    /// jitter 증가 허용 (비율, e.g. 0.5 → 이전 대비 +50% 이상이면 REGRESS)
    #[serde(default = "default_jitter_ratio")]
    pub jitter_increase_ratio: f64,

    /// OOO 증가 허용 (비율, e.g. 1.0 → 이전 대비 +100% 이상이면 REGRESS)
    #[serde(default = "default_ooo_ratio")]
    pub ooo_increase_ratio: f64,

    /// floor grant latency 증가 허용 (절대 ms)
    #[serde(default = "default_floor_latency_delta")]
    pub floor_grant_latency_delta_ms: f64,
}

fn default_loss_delta() -> f64 { 2.0 }
fn default_jitter_ratio() -> f64 { 0.5 }
fn default_ooo_ratio() -> f64 { 1.0 }
fn default_floor_latency_delta() -> f64 { 100.0 }

impl Default for RegressionThresholds {
    fn default() -> Self {
        Self {
            loss_rate_delta: default_loss_delta(),
            jitter_increase_ratio: default_jitter_ratio(),
            ooo_increase_ratio: default_ooo_ratio(),
            floor_grant_latency_delta_ms: default_floor_latency_delta(),
        }
    }
}

impl RegressionThresholds {
    /// 엄격 기준
    pub fn strict() -> Self {
        Self {
            loss_rate_delta: 0.5,
            jitter_increase_ratio: 0.2,
            ooo_increase_ratio: 0.5,
            floor_grant_latency_delta_ms: 50.0,
        }
    }

    /// TOML 파일에서 로드
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let t: Self = toml::from_str(&content)?;
        Ok(t)
    }

    /// 이름으로 builtin 또는 파일 로드
    pub fn resolve(name: &str) -> Self {
        match name {
            "default" => Self::default(),
            "strict" => Self::strict(),
            other => {
                let path = std::path::Path::new("judgements").join(format!("{}.toml", other));
                Self::load(&path).unwrap_or_else(|e| {
                    tracing::warn!("failed to load regression thresholds '{}': {}, using default", other, e);
                    Self::default()
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_thresholds() {
        let t = RegressionThresholds::default();
        assert_eq!(t.loss_rate_delta, 2.0);
        assert_eq!(t.ooo_increase_ratio, 1.0);
    }

    #[test]
    fn parse_toml() {
        let toml = r#"
loss_rate_delta = 1.0
jitter_increase_ratio = 0.3
ooo_increase_ratio = 0.5
floor_grant_latency_delta_ms = 80.0
"#;
        let t: RegressionThresholds = toml::from_str(toml).unwrap();
        assert_eq!(t.loss_rate_delta, 1.0);
        assert_eq!(t.floor_grant_latency_delta_ms, 80.0);
    }
}
