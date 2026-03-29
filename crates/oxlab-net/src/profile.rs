// author: kodeholic (powered by Claude)
//! Network profile — TOML 기반 네트워크 프로파일 로더

use crate::filter::FilterConfig;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkProfile {
    pub meta: ProfileMeta,
    pub conditions: FilterConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileMeta {
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// "preset" | "snapshot" | "custom"
    #[serde(default = "default_origin")]
    pub origin: String,
}

fn default_origin() -> String {
    "preset".to_string()
}

impl NetworkProfile {
    /// TOML 파일에서 프로파일 로드
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let profile: NetworkProfile = toml::from_str(&content)?;
        info!(
            "loaded network profile: {} ({})",
            profile.meta.name, profile.meta.origin
        );
        Ok(profile)
    }

    /// 이름으로 profiles/ 디렉토리에서 검색
    pub fn load_by_name(
        profiles_dir: &Path,
        name: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let path = profiles_dir.join(format!("{}.toml", name));
        Self::load(&path)
    }
}

// ── 빌트인 프리셋 (파일 없이 사용 가능) ──

impl NetworkProfile {
    pub fn pristine() -> Self {
        Self {
            meta: ProfileMeta {
                name: "pristine".into(),
                description: "Ideal network — no impairment".into(),
                origin: "builtin".into(),
            },
            conditions: FilterConfig::default(),
        }
    }

    pub fn office_wifi() -> Self {
        Self {
            meta: ProfileMeta {
                name: "office_wifi".into(),
                description: "Office Wi-Fi".into(),
                origin: "builtin".into(),
            },
            conditions: FilterConfig {
                loss_percent: 0.5,
                delay_ms: 5,
                jitter_ms: 10,
                bandwidth_kbps: 50_000,
            },
        }
    }

    pub fn field_lte() -> Self {
        Self {
            meta: ProfileMeta {
                name: "field_lte".into(),
                description: "Field LTE — good signal".into(),
                origin: "builtin".into(),
            },
            conditions: FilterConfig {
                loss_percent: 2.0,
                delay_ms: 50,
                jitter_ms: 30,
                bandwidth_kbps: 10_000,
            },
        }
    }

    pub fn field_lte_poor() -> Self {
        Self {
            meta: ProfileMeta {
                name: "field_lte_poor".into(),
                description: "Field LTE — poor signal".into(),
                origin: "builtin".into(),
            },
            conditions: FilterConfig {
                loss_percent: 8.0,
                delay_ms: 100,
                jitter_ms: 80,
                bandwidth_kbps: 3_000,
            },
        }
    }

    pub fn basement() -> Self {
        Self {
            meta: ProfileMeta {
                name: "basement".into(),
                description: "Basement / elevator".into(),
                origin: "builtin".into(),
            },
            conditions: FilterConfig {
                loss_percent: 15.0,
                delay_ms: 150,
                jitter_ms: 100,
                bandwidth_kbps: 1_000,
            },
        }
    }

    /// 이름으로 빌트인 프리셋 검색 (없으면 None)
    pub fn builtin(name: &str) -> Option<Self> {
        match name {
            "pristine" => Some(Self::pristine()),
            "office_wifi" => Some(Self::office_wifi()),
            "field_lte" => Some(Self::field_lte()),
            "field_lte_poor" => Some(Self::field_lte_poor()),
            "basement" => Some(Self::basement()),
            _ => None,
        }
    }
}
