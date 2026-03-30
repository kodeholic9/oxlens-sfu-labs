// author: kodeholic (powered by Claude)
//! oxlab-judge — 2계층 판정기
//!
//! Layer 1: SFU 행동 검증 (binary pass/fail, 정답 있음)
//!   → 실기기 시험 전 gate. 하나라도 FAIL이면 실기기에 안 간다.
//!
//! Layer 2: 열화 내성 검증 (상대적, 이전 대비 회귀 감지)
//!   → 절대 기준 없이 "이전보다 나빠졌는가"만 감지.
//!
//! 설계 문서: context/OXLABS_DESIGN.md §4.4

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

pub mod checkpoint_registry;
pub mod thresholds;

pub use thresholds::RegressionThresholds;

// ============================================================================
// 공통 타입
// ============================================================================

/// 최종 판정 (binary)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    Pass,
    Fail,
}

impl std::fmt::Display for Verdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Verdict::Pass => write!(f, "PASS"),
            Verdict::Fail => write!(f, "FAIL"),
        }
    }
}

/// 회귀 판정 (상대적)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegressionVerdict {
    /// 이전과 동일 범위
    Stable,
    /// 개선됨
    Improved,
    /// 나빠짐
    Regressed,
    /// 비교 대상 없음 (첫 실행)
    NoBaseline,
}

impl std::fmt::Display for RegressionVerdict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegressionVerdict::Stable => write!(f, "STABLE"),
            RegressionVerdict::Improved => write!(f, "IMPROVED"),
            RegressionVerdict::Regressed => write!(f, "REGRESS"),
            RegressionVerdict::NoBaseline => write!(f, "NO_BASELINE"),
        }
    }
}

// ============================================================================
// Layer 1: SFU 행동 검증 (binary)
// ============================================================================

/// 체크포인트 카테고리
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CheckpointCategory {
    RtcpTerminator,
    SrTranslation,
    PttRelay,
    PliGovernor,
    SubscriberGate,
    FloorControl,
    CoreRelay,
    Simulcast,
    ScreenShare,
    Lifecycle,
}

impl std::fmt::Display for CheckpointCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::RtcpTerminator => "RTCP Terminator",
            Self::SrTranslation  => "SR Translation",
            Self::PttRelay       => "PTT Relay",
            Self::PliGovernor    => "PLI Governor",
            Self::SubscriberGate => "SubscriberGate",
            Self::FloorControl   => "Floor Control",
            Self::CoreRelay      => "Core Relay",
            Self::Simulcast      => "Simulcast",
            Self::ScreenShare    => "Screen Share",
            Self::Lifecycle      => "Lifecycle",
        };
        write!(f, "{}", s)
    }
}

impl CheckpointCategory {
    /// 문자열에서 파싱 (시나리오 TOML용)
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "rtcp_terminator" | "rtcpterminator" => Some(Self::RtcpTerminator),
            "sr_translation" | "srtranslation" => Some(Self::SrTranslation),
            "ptt_relay" | "pttrelay" => Some(Self::PttRelay),
            "pli_governor" | "pligovernor" => Some(Self::PliGovernor),
            "subscriber_gate" | "subscribergate" => Some(Self::SubscriberGate),
            "floor_control" | "floorcontrol" => Some(Self::FloorControl),
            "core_relay" | "corerelay" => Some(Self::CoreRelay),
            "simulcast" => Some(Self::Simulcast),
            "screen_share" | "screenshare" => Some(Self::ScreenShare),
            "lifecycle" => Some(Self::Lifecycle),
            _ => None,
        }
    }
}

/// 단일 체크포인트 실행 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointResult {
    /// 체크포인트 ID (e.g. "L1-04")
    pub id: String,
    /// 체크포인트 이름
    pub name: String,
    /// 카테고리
    pub category: CheckpointCategory,
    /// 판정
    pub verdict: Verdict,
    /// 기대값 (e.g. "0 RR relayed")
    pub expected: String,
    /// 실제 관측값 (e.g. "3 RR leaked")
    pub actual: String,
}

impl CheckpointResult {
    /// PASS 결과 생성 헬퍼
    pub fn pass(id: &str, name: &str, category: CheckpointCategory, expected: &str, actual: &str) -> Self {
        Self {
            id: id.to_string(),
            name: name.to_string(),
            category,
            verdict: Verdict::Pass,
            expected: expected.to_string(),
            actual: actual.to_string(),
        }
    }

    /// FAIL 결과 생성 헬퍼
    pub fn fail(id: &str, name: &str, category: CheckpointCategory, expected: &str, actual: &str) -> Self {
        Self {
            id: id.to_string(),
            name: name.to_string(),
            category,
            verdict: Verdict::Fail,
            expected: expected.to_string(),
            actual: actual.to_string(),
        }
    }

    /// 레지스트리 정의에서 PASS 생성
    pub fn pass_from_def(def: &checkpoint_registry::CheckpointDef, actual: &str) -> Self {
        Self::pass(def.id, def.name, def.category, def.description, actual)
    }

    /// 레지스트리 정의에서 FAIL 생성
    pub fn fail_from_def(def: &checkpoint_registry::CheckpointDef, actual: &str) -> Self {
        Self::fail(def.id, def.name, def.category, def.description, actual)
    }
}

/// Layer 1 전체 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Layer1Result {
    pub checkpoints: Vec<CheckpointResult>,
    /// 하나라도 FAIL → FAIL
    pub verdict: Verdict,
    pub passed: usize,
    pub failed: usize,
    /// 해당 시나리오에 적용 불가한 항목 수
    pub skipped: usize,
}

impl Layer1Result {
    /// 콘솔 출력
    pub fn print_summary(&self) {
        let icon = if self.verdict == Verdict::Pass { "✓" } else { "✗" };
        println!("  Layer 1 [{}] {}: {}/{} passed, {} failed, {} skipped",
            icon, self.verdict, self.passed, self.passed + self.failed, self.failed, self.skipped);

        for cp in &self.checkpoints {
            if cp.verdict == Verdict::Fail {
                println!("    ✗ {} [{}] {}", cp.id, cp.category, cp.name);
                println!("      expected: {}", cp.expected);
                println!("      actual:   {}", cp.actual);
            }
        }
    }
}

// ============================================================================
// Layer 2: 열화 내성 (회귀 감지)
// ============================================================================

/// 단일 메트릭의 회귀 비교
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionMetric {
    pub name: String,
    pub current: f64,
    pub baseline: Option<f64>,
    pub delta: Option<f64>,
    pub threshold: f64,
    pub verdict: RegressionVerdict,
}

/// 참가자별 측정 수치 (봇 recv 메트릭에서 추출)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParticipantMetrics {
    pub total_packets: u64,
    pub total_lost: u64,
    pub loss_rate_percent: f64,
    pub max_jitter: f64,
    pub total_ooo: u64,
}

/// Layer 2 참가자별 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Layer2Participant {
    pub id: String,
    pub metrics: ParticipantMetrics,
    pub regression: Vec<RegressionMetric>,
    pub verdict: RegressionVerdict,
}

/// Layer 2 전체 결과
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Layer2Result {
    pub participants: Vec<Layer2Participant>,
    /// 하나라도 Regressed → Regressed
    pub verdict: RegressionVerdict,
}

impl Layer2Result {
    /// 콘솔 출력
    pub fn print_summary(&self) {
        let icon = match self.verdict {
            RegressionVerdict::Regressed => "✗",
            RegressionVerdict::Improved => "▲",
            _ => "─",
        };
        println!("  Layer 2 [{}] {}", icon, self.verdict);

        for p in &self.participants {
            println!("    [{}] {} | rx={} lost={} loss={:.2}% jitter={:.0} ooo={}",
                p.id, p.verdict,
                p.metrics.total_packets, p.metrics.total_lost,
                p.metrics.loss_rate_percent, p.metrics.max_jitter, p.metrics.total_ooo);

            for rm in &p.regression {
                if rm.verdict == RegressionVerdict::Regressed {
                    println!("      ✗ {} current={:.2} baseline={:.2} Δ={:+.2} (threshold={:.2})",
                        rm.name, rm.current,
                        rm.baseline.unwrap_or(0.0),
                        rm.delta.unwrap_or(0.0),
                        rm.threshold);
                }
            }
        }
    }
}

// ============================================================================
// JudgeReport v2 — 2계층 통합
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JudgeReport {
    pub scenario: String,
    pub profile: String,
    pub timestamp: String,

    /// Layer 1: SFU 행동 (binary)
    pub layer1: Layer1Result,

    /// Layer 2: 열화 내성 (회귀)
    pub layer2: Layer2Result,

    /// 최종 판정: L1 FAIL → FAIL, L1 PASS + L2 REGRESS → FAIL
    pub overall: Verdict,

    /// 한 줄 요약: "L1:21/21 L2:STABLE"
    pub summary: String,

    /// 실행 중 에러
    pub errors: Vec<String>,
}

impl JudgeReport {
    /// JSON 직렬화
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }

    /// 콘솔 출력
    pub fn print_summary(&self) {
        let icon = if self.overall == Verdict::Pass { "✓" } else { "✗" };
        println!("\n═══ VERDICT: {} {} ═══", icon, self.overall);
        println!("  scenario: {}", self.scenario);
        println!("  profile: {}", self.profile);
        println!("  summary: {}", self.summary);
        println!();

        self.layer1.print_summary();
        println!();
        self.layer2.print_summary();

        if !self.errors.is_empty() {
            println!("\n  errors: {}", self.errors.len());
            for e in &self.errors {
                println!("    → {}", e);
            }
        }
    }
}

// ============================================================================
// RecvInput — 봇 메트릭 입력 (oxlab-bot 비의존)
// ============================================================================

/// 판정기 입력용 수신 메트릭
#[derive(Debug, Clone)]
pub struct RecvInput {
    pub total_packets: u64,
    pub total_lost: u64,
    pub total_ooo: u64,
    pub streams: Vec<StreamInput>,
}

#[derive(Debug, Clone)]
pub struct StreamInput {
    pub ssrc: u32,
    pub pt: u8,
    pub packets_received: u64,
    pub packets_lost: u64,
    pub out_of_order: u64,
    pub jitter: f64,
}

// ============================================================================
// Layer 1 baseline (이전 결과 로드/저장)
// ============================================================================

/// Layer 2 비교용 이전 실행 결과 (baseline)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineEntry {
    pub participant_id: String,
    pub loss_rate_percent: f64,
    pub max_jitter: f64,
    pub total_ooo: u64,
}

/// baseline 파일 포맷
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineFile {
    pub scenario: String,
    pub profile: String,
    pub timestamp: String,
    pub participants: Vec<BaselineEntry>,
}

impl BaselineFile {
    /// JSON 파일에서 로드
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&content)?)
    }

    /// JSON 파일로 저장
    pub fn save(&self, path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// 참가자별 baseline 조회
    pub fn find(&self, participant_id: &str) -> Option<&BaselineEntry> {
        self.participants.iter().find(|e| e.participant_id == participant_id)
    }
}

// ============================================================================
// 판정 함수
// ============================================================================

/// Layer 1 판정: 체크포인트 결과 집계
///
/// 체크포인트 평가 자체는 호출부(봇 관측 포인트)에서 수행.
/// 여기서는 결과를 모아서 집계만 한다.
pub fn judge_layer1(
    checkpoints: Vec<CheckpointResult>,
    total_applicable: usize,
) -> Layer1Result {
    let passed = checkpoints.iter().filter(|c| c.verdict == Verdict::Pass).count();
    let failed = checkpoints.iter().filter(|c| c.verdict == Verdict::Fail).count();
    let skipped = total_applicable.saturating_sub(passed + failed);

    let verdict = if failed > 0 { Verdict::Fail } else { Verdict::Pass };

    Layer1Result { checkpoints, verdict, passed, failed, skipped }
}

/// Layer 2 판정: 수신 메트릭 + 이전 baseline 대비 회귀 감지
pub fn judge_layer2(
    participants: &HashMap<String, RecvInput>,
    baseline: Option<&BaselineFile>,
    thresholds: &RegressionThresholds,
    errors: &[String],
) -> Layer2Result {
    let mut l2_participants = Vec::new();
    let mut any_regressed = false;
    let has_baseline = baseline.is_some();

    for (id, recv) in participants {
        let total = recv.total_packets + recv.total_lost;
        let loss_pct = if total > 0 {
            (recv.total_lost as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        let max_jitter = recv.streams.iter()
            .map(|s| s.jitter)
            .fold(0.0f64, f64::max);

        let metrics = ParticipantMetrics {
            total_packets: recv.total_packets,
            total_lost: recv.total_lost,
            loss_rate_percent: loss_pct,
            max_jitter,
            total_ooo: recv.total_ooo,
        };

        // baseline 비교
        let prev = baseline.and_then(|b| b.find(id));
        let mut regression_metrics = Vec::new();
        let mut participant_regressed = false;

        // loss rate (절대 차이)
        let (loss_verdict, loss_delta) = match prev {
            Some(p) => {
                let delta = loss_pct - p.loss_rate_percent;
                let v = if delta > thresholds.loss_rate_delta {
                    participant_regressed = true;
                    RegressionVerdict::Regressed
                } else if delta < -thresholds.loss_rate_delta {
                    RegressionVerdict::Improved
                } else {
                    RegressionVerdict::Stable
                };
                (v, Some(delta))
            }
            None => (RegressionVerdict::NoBaseline, None),
        };
        regression_metrics.push(RegressionMetric {
            name: "loss_rate".to_string(),
            current: loss_pct,
            baseline: prev.map(|p| p.loss_rate_percent),
            delta: loss_delta,
            threshold: thresholds.loss_rate_delta,
            verdict: loss_verdict,
        });

        // jitter (비율 증가)
        let (jitter_verdict, jitter_delta) = match prev {
            Some(p) if p.max_jitter > 0.0 => {
                let ratio = (max_jitter - p.max_jitter) / p.max_jitter;
                let v = if ratio > thresholds.jitter_increase_ratio {
                    participant_regressed = true;
                    RegressionVerdict::Regressed
                } else if ratio < -thresholds.jitter_increase_ratio {
                    RegressionVerdict::Improved
                } else {
                    RegressionVerdict::Stable
                };
                (v, Some(ratio))
            }
            Some(_) => (RegressionVerdict::Stable, Some(0.0)),
            None => (RegressionVerdict::NoBaseline, None),
        };
        regression_metrics.push(RegressionMetric {
            name: "jitter".to_string(),
            current: max_jitter,
            baseline: prev.map(|p| p.max_jitter),
            delta: jitter_delta,
            threshold: thresholds.jitter_increase_ratio,
            verdict: jitter_verdict,
        });

        // OOO (비율 증가)
        let (ooo_verdict, ooo_delta) = match prev {
            Some(p) if p.total_ooo > 0 => {
                let ratio = (recv.total_ooo as f64 - p.total_ooo as f64) / p.total_ooo as f64;
                let v = if ratio > thresholds.ooo_increase_ratio {
                    participant_regressed = true;
                    RegressionVerdict::Regressed
                } else if ratio < -thresholds.ooo_increase_ratio {
                    RegressionVerdict::Improved
                } else {
                    RegressionVerdict::Stable
                };
                (v, Some(ratio))
            }
            Some(_) => {
                // baseline ooo가 0이었는데 현재도 0이면 stable, 증가했으면 regress
                if recv.total_ooo > 0 {
                    participant_regressed = true;
                    (RegressionVerdict::Regressed, Some(recv.total_ooo as f64))
                } else {
                    (RegressionVerdict::Stable, Some(0.0))
                }
            }
            None => (RegressionVerdict::NoBaseline, None),
        };
        regression_metrics.push(RegressionMetric {
            name: "ooo".to_string(),
            current: recv.total_ooo as f64,
            baseline: prev.map(|p| p.total_ooo as f64),
            delta: ooo_delta,
            threshold: thresholds.ooo_increase_ratio,
            verdict: ooo_verdict,
        });

        if participant_regressed {
            any_regressed = true;
        }

        let p_verdict = if participant_regressed {
            RegressionVerdict::Regressed
        } else if !has_baseline {
            RegressionVerdict::NoBaseline
        } else {
            // 하나라도 improved면 improved, 아니면 stable
            if regression_metrics.iter().any(|m| m.verdict == RegressionVerdict::Improved) {
                RegressionVerdict::Improved
            } else {
                RegressionVerdict::Stable
            }
        };

        l2_participants.push(Layer2Participant {
            id: id.clone(),
            metrics,
            regression: regression_metrics,
            verdict: p_verdict,
        });
    }

    // 정렬 (id 순)
    l2_participants.sort_by(|a, b| a.id.cmp(&b.id));

    // connection 에러 시 모든 참가자에 반영
    if !errors.is_empty() {
        any_regressed = true;
    }

    let verdict = if any_regressed {
        RegressionVerdict::Regressed
    } else if !has_baseline {
        RegressionVerdict::NoBaseline
    } else if l2_participants.iter().any(|p| p.verdict == RegressionVerdict::Improved) {
        RegressionVerdict::Improved
    } else {
        RegressionVerdict::Stable
    };

    Layer2Result { participants: l2_participants, verdict }
}

/// 통합 판정
///
/// - L1 FAIL → overall FAIL (gate 불통과)
/// - L1 PASS + L2 REGRESS → overall FAIL (회귀 감지)
/// - L1 PASS + L2 STABLE/IMPROVED/NO_BASELINE → overall PASS
pub fn judge(
    scenario_name: &str,
    profile_name: &str,
    layer1_checkpoints: Vec<CheckpointResult>,
    total_applicable_checkpoints: usize,
    recv_inputs: &HashMap<String, RecvInput>,
    baseline: Option<&BaselineFile>,
    regression_thresholds: &RegressionThresholds,
    errors: &[String],
) -> JudgeReport {
    let layer1 = judge_layer1(layer1_checkpoints, total_applicable_checkpoints);
    let layer2 = judge_layer2(recv_inputs, baseline, regression_thresholds, errors);

    // overall 판정
    let overall = if layer1.verdict == Verdict::Fail {
        Verdict::Fail
    } else if layer2.verdict == RegressionVerdict::Regressed {
        Verdict::Fail
    } else {
        Verdict::Pass
    };

    // summary 생성
    let l1_summary = if layer1.failed > 0 {
        let failed_ids: Vec<String> = layer1.checkpoints.iter()
            .filter(|c| c.verdict == Verdict::Fail)
            .map(|c| c.id.clone())
            .collect();
        format!("L1:{}/{}[FAIL:{}]",
            layer1.passed, layer1.passed + layer1.failed,
            failed_ids.join(","))
    } else {
        format!("L1:{}/{}", layer1.passed, layer1.passed + layer1.failed)
    };

    let l2_summary = match layer2.verdict {
        RegressionVerdict::Regressed => {
            let regressed_metrics: Vec<String> = layer2.participants.iter()
                .flat_map(|p| p.regression.iter())
                .filter(|m| m.verdict == RegressionVerdict::Regressed)
                .map(|m| m.name.clone())
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();
            format!("L2:REGRESS[{}]", regressed_metrics.join(","))
        }
        v => format!("L2:{}", v),
    };

    let summary = format!("{} {}", l1_summary, l2_summary);

    JudgeReport {
        scenario: scenario_name.to_string(),
        profile: profile_name.to_string(),
        timestamp: timestamp_now(),
        layer1,
        layer2,
        overall,
        summary,
        errors: errors.to_vec(),
    }
}

/// 현재 결과에서 baseline 파일 생성 (다음 회귀 비교용)
pub fn create_baseline(report: &JudgeReport) -> BaselineFile {
    BaselineFile {
        scenario: report.scenario.clone(),
        profile: report.profile.clone(),
        timestamp: report.timestamp.clone(),
        participants: report.layer2.participants.iter().map(|p| {
            BaselineEntry {
                participant_id: p.id.clone(),
                loss_rate_percent: p.metrics.loss_rate_percent,
                max_jitter: p.metrics.max_jitter,
                total_ooo: p.metrics.total_ooo,
            }
        }).collect(),
    }
}

fn timestamp_now() -> String {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}s", d.as_secs())
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layer1_all_pass() {
        let cps = vec![
            CheckpointResult::pass("L1-01", "test", CheckpointCategory::RtcpTerminator, "0", "0"),
            CheckpointResult::pass("L1-04", "test", CheckpointCategory::PttRelay, "0", "0"),
        ];
        let result = judge_layer1(cps, 5);
        assert_eq!(result.verdict, Verdict::Pass);
        assert_eq!(result.passed, 2);
        assert_eq!(result.failed, 0);
        assert_eq!(result.skipped, 3);
    }

    #[test]
    fn layer1_one_fail_means_fail() {
        let cps = vec![
            CheckpointResult::pass("L1-01", "test", CheckpointCategory::RtcpTerminator, "0", "0"),
            CheckpointResult::fail("L1-04", "gating", CheckpointCategory::PttRelay, "0 packets", "3 packets leaked"),
        ];
        let result = judge_layer1(cps, 2);
        assert_eq!(result.verdict, Verdict::Fail);
        assert_eq!(result.passed, 1);
        assert_eq!(result.failed, 1);
    }

    #[test]
    fn layer2_no_baseline() {
        let mut participants = HashMap::new();
        participants.insert("bot_1".to_string(), RecvInput {
            total_packets: 1000, total_lost: 10, total_ooo: 2,
            streams: vec![StreamInput { ssrc: 1, pt: 111, packets_received: 1000, packets_lost: 10, out_of_order: 2, jitter: 5.0 }],
        });
        let result = judge_layer2(&participants, None, &RegressionThresholds::default(), &[]);
        assert_eq!(result.verdict, RegressionVerdict::NoBaseline);
    }

    #[test]
    fn layer2_regression_detected() {
        let mut participants = HashMap::new();
        participants.insert("bot_1".to_string(), RecvInput {
            total_packets: 1000, total_lost: 80, total_ooo: 5,
            streams: vec![StreamInput { ssrc: 1, pt: 111, packets_received: 1000, packets_lost: 80, out_of_order: 5, jitter: 50.0 }],
        });
        let baseline = BaselineFile {
            scenario: "test".into(), profile: "pristine".into(), timestamp: "0s".into(),
            participants: vec![BaselineEntry {
                participant_id: "bot_1".into(),
                loss_rate_percent: 1.0,
                max_jitter: 5.0,
                total_ooo: 0,
            }],
        };
        let result = judge_layer2(&participants, Some(&baseline), &RegressionThresholds::default(), &[]);
        assert_eq!(result.verdict, RegressionVerdict::Regressed);
    }

    #[test]
    fn overall_l1_fail_is_fail() {
        let cps = vec![
            CheckpointResult::fail("L1-01", "test", CheckpointCategory::RtcpTerminator, "0", "3"),
        ];
        let recv = HashMap::new();
        let report = judge("test", "pristine", cps, 1, &recv, None, &RegressionThresholds::default(), &[]);
        assert_eq!(report.overall, Verdict::Fail);
        assert!(report.summary.contains("FAIL:L1-01"));
    }

    #[test]
    fn overall_all_pass() {
        let cps = vec![
            CheckpointResult::pass("L1-01", "test", CheckpointCategory::RtcpTerminator, "0", "0"),
        ];
        let recv = HashMap::new();
        let report = judge("test", "pristine", cps, 1, &recv, None, &RegressionThresholds::default(), &[]);
        assert_eq!(report.overall, Verdict::Pass);
        assert!(report.summary.contains("L1:1/1"));
    }

    #[test]
    fn summary_format() {
        let cps = vec![
            CheckpointResult::pass("L1-01", "t", CheckpointCategory::RtcpTerminator, "", ""),
            CheckpointResult::fail("L1-04", "t", CheckpointCategory::PttRelay, "", ""),
            CheckpointResult::fail("L1-07", "t", CheckpointCategory::PttRelay, "", ""),
        ];
        let recv = HashMap::new();
        let report = judge("test", "pristine", cps, 3, &recv, None, &RegressionThresholds::default(), &[]);
        assert!(report.summary.contains("L1:1/3[FAIL:L1-04,L1-07]"));
    }
}
