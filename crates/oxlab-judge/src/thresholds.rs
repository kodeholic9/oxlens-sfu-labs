// author: kodeholic (powered by Claude)
//! Thresholds — 판정 기준 (TOML 로드 가능)

use serde::{Deserialize, Serialize};

/// 판정 기준 임계치
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Thresholds {
    /// 최대 허용 loss rate (%, 이상이면 FAIL)
    #[serde(default = "default_max_loss")]
    pub max_loss_rate_percent: f64,
    /// 최대 허용 jitter (RTP timestamp 단위, 이상이면 FAIL)
    #[serde(default = "default_max_jitter")]
    pub max_jitter: f64,
    /// 최대 허용 out-of-order 패킷 수
    #[serde(default = "default_max_ooo")]
    pub max_ooo_count: u64,
}

fn default_max_loss() -> f64 { 5.0 }
fn default_max_jitter() -> f64 { 500.0 }
fn default_max_ooo() -> u64 { 50 }

impl Default for Thresholds {
    fn default() -> Self {
        Self {
            max_loss_rate_percent: default_max_loss(),
            max_jitter: default_max_jitter(),
            max_ooo_count: default_max_ooo(),
        }
    }
}

impl Thresholds {
    /// 엄격 기준 (pristine 환경 검증용)
    pub fn strict() -> Self {
        Self {
            max_loss_rate_percent: 0.1,
            max_jitter: 100.0,
            max_ooo_count: 5,
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
                    tracing::warn!("failed to load judgement '{}': {}, using default", other, e);
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
        let t = Thresholds::default();
        assert_eq!(t.max_loss_rate_percent, 5.0);
        assert_eq!(t.max_ooo_count, 50);
    }

    #[test]
    fn parse_toml() {
        let toml = r#"
max_loss_rate_percent = 2.0
max_jitter = 200.0
max_ooo_count = 10
"#;
        let t: Thresholds = toml::from_str(toml).unwrap();
        assert_eq!(t.max_loss_rate_percent, 2.0);
        assert_eq!(t.max_ooo_count, 10);
    }
}
