// author: kodeholic (powered by Claude)
//! oxlab-scenario — 시나리오 엔진
//!
//! TOML 시나리오 파싱 → 봇 오케스트레이션 → 타임라인 실행 → 메트릭 수집.
//! 설계 문서: context/OXLABS_DESIGN.md §3.3

pub mod model;
pub mod engine;

pub use engine::{ScenarioEngine, ScenarioResult};
pub use model::{Scenario, ScenarioMeta, ParticipantDef, Action, ActionType};
pub use oxlab_judge;

/// 시나리오 파일 경로로 실행
pub async fn run(scenario_path: &str) -> Result<ScenarioResult, Box<dyn std::error::Error>> {
    let path = std::path::Path::new(scenario_path);
    let scenario = Scenario::load(path)?;
    let engine = ScenarioEngine::new(scenario);
    Ok(engine.run().await)
}
