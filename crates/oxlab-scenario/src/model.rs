// author: kodeholic (powered by Claude)
//! Scenario TOML data model
//!
//! 설계 문서 §3.3 시나리오 TOML 포맷 구현.

use serde::Deserialize;

/// 시나리오 최상위 구조
#[derive(Debug, Deserialize)]
pub struct Scenario {
    pub meta: ScenarioMeta,
    pub participants: Vec<ParticipantDef>,
    pub actions: Vec<Action>,
}

/// 시나리오 메타 정보
#[derive(Debug, Deserialize)]
pub struct ScenarioMeta {
    pub name: String,
    #[serde(default)]
    pub description: String,
    /// SFU 서버 주소 (기본: 127.0.0.1)
    #[serde(default = "default_server")]
    pub server: String,
    /// WS 포트 (기본: 9222)
    #[serde(default = "default_ws_port")]
    pub ws_port: u16,
    /// 방 이름
    #[serde(default = "default_room")]
    pub room: String,
    /// 방 모드 (conference | ptt)
    #[serde(default = "default_mode")]
    pub mode: String,
    /// 판정 기준 파일명 (judgements/ 디렉토리 내)
    #[serde(default)]
    pub judgement: Option<String>,
    /// Layer 1 체크포인트 카테고리 필터 (e.g. ["PttRelay", "FloorControl"])
    /// 비어있으면 전체 카테고리 적용
    #[serde(default)]
    pub categories: Vec<String>,
}

fn default_server() -> String { "127.0.0.1".to_string() }
fn default_ws_port() -> u16 { 1974 }
fn default_room() -> String { "scenario-test".to_string() }
fn default_mode() -> String { "conference".to_string() }

/// 참가자 정의
#[derive(Debug, Deserialize)]
pub struct ParticipantDef {
    pub id: String,
    /// 네트워크 프로파일 (builtin 이름 또는 TOML 파일)
    #[serde(default = "default_profile")]
    pub profile: String,
    /// 발행 미디어 종류 (예: ["video", "audio"])
    #[serde(default)]
    pub publish: Vec<String>,
    /// 구독 미디어 종류 (현재 미사용, 향후 subscribe-only 봇)
    #[serde(default)]
    pub subscribe: Vec<String>,
    /// 역할 (PttSpeaker, PttListener 등, 향후 확장)
    #[serde(default)]
    pub role: Option<String>,
}

fn default_profile() -> String { "pristine".to_string() }

/// 시간축 액션
#[derive(Debug, Deserialize)]
pub struct Action {
    /// 실행 시점 (시나리오 시작 후 초)
    pub at_sec: f64,
    /// 액션 타입
    #[serde(rename = "type")]
    pub action_type: ActionType,

    // ── 액션별 선택 필드 ──

    /// 대상 참가자 ID (ptt_request, ptt_release, network_transition, kill_bot)
    #[serde(default)]
    pub actor: Option<String>,
    /// 복수 대상 (ptt_alternate)
    #[serde(default)]
    pub actors: Option<Vec<String>>,
    /// PTT 우선순위 (ptt_request)
    #[serde(default)]
    pub priority: Option<u8>,
    /// 네트워크 프로파일 이름 (network_transition)
    #[serde(default)]
    pub profile: Option<String>,
    /// 반복 횟수 (ptt_alternate)
    #[serde(default)]
    pub count: Option<u32>,
    /// 반복 간격 초 (ptt_alternate)
    #[serde(default)]
    pub interval_sec: Option<f64>,
    /// 대기 시간 초 (wait)
    #[serde(default)]
    pub duration_sec: Option<f64>,
}

/// 지원하는 액션 타입
#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActionType {
    /// 전체 봇 접속 + 방 입장
    AllJoin,
    /// 전체 봇 미디어 셋업 + publishing 시작
    StartMedia,
    /// 특정 봇 PTT 발화권 요청
    PttRequest,
    /// 특정 봇 PTT 발화권 해제
    PttRelease,
    /// 복수 봇 교대 발화 (actors + interval_sec + count)
    PttAlternate,
    /// 특정 봇 네트워크 프로파일 동적 전환
    NetworkTransition,
    /// 특정 봇 강제 종료 (좀비 시뮬레이션)
    KillBot,
    /// 대기 (heartbeat + process_events 포함)
    Wait,
}

impl Scenario {
    /// TOML 문자열에서 파싱
    pub fn from_toml(content: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(content)
    }

    /// TOML 파일에서 로드
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        Ok(Self::from_toml(&content)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_scenario() {
        let toml = r#"
[meta]
name = "test"

[[participants]]
id = "bot_1"

[[actions]]
at_sec = 0
type = "all_join"
"#;
        let s = Scenario::from_toml(toml).unwrap();
        assert_eq!(s.meta.name, "test");
        assert_eq!(s.participants.len(), 1);
        assert_eq!(s.actions.len(), 1);
        assert_eq!(s.actions[0].action_type, ActionType::AllJoin);
    }

    #[test]
    fn parse_ptt_scenario() {
        let toml = r#"
[meta]
name = "ptt_rapid"
description = "PTT rapid switching"
mode = "ptt"

[[participants]]
id = "speaker_a"
profile = "field_lte"
publish = ["video", "audio"]

[[participants]]
id = "speaker_b"
profile = "field_lte_poor"
publish = ["video", "audio"]

[[actions]]
at_sec = 0
type = "all_join"

[[actions]]
at_sec = 2
type = "start_media"

[[actions]]
at_sec = 5
type = "ptt_request"
actor = "speaker_a"
priority = 0

[[actions]]
at_sec = 8
type = "ptt_release"
actor = "speaker_a"

[[actions]]
at_sec = 10
type = "ptt_alternate"
actors = ["speaker_a", "speaker_b"]
interval_sec = 3.0
count = 5

[[actions]]
at_sec = 30
type = "network_transition"
actor = "speaker_b"
profile = "basement"

[[actions]]
at_sec = 35
type = "wait"
duration_sec = 5.0
"#;
        let s = Scenario::from_toml(toml).unwrap();
        assert_eq!(s.meta.mode, "ptt");
        assert_eq!(s.participants.len(), 2);
        assert_eq!(s.actions.len(), 7);
        assert_eq!(s.actions[4].action_type, ActionType::PttAlternate);
        assert_eq!(s.actions[4].count, Some(5));
    }
}
