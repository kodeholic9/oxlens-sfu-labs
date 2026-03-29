// author: kodeholic (powered by Claude)
//! oxlab-judge — 판정기
//!
//! 봇 수신 메트릭 + 시나리오 에러 → 임계치 기반 pass/fail 판정.
//! MVS의 16자리 detail 벡터에서 영감.
//! 단순 pass/fail이 아니라 실패 차원을 다차원으로 분류.

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

pub mod thresholds;
pub use thresholds::Thresholds;

// ============================================================================
// Verdict
// ============================================================================

/// 최종 판정
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

// ============================================================================
// DetailVerdict — 차원별 상세 판정
// ============================================================================

/// 차원별 상세 판정 (MVS의 detail[16] 대응)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailVerdict {
    pub loss_rate: Verdict,
    pub jitter: Verdict,
    pub out_of_order: Verdict,
    pub connection: Verdict,
}

impl DetailVerdict {
    /// 모든 차원이 Pass면 Pass
    pub fn overall(&self) -> Verdict {
        let all = [
            self.loss_rate,
            self.jitter,
            self.out_of_order,
            self.connection,
        ];
        if all.iter().all(|v| *v == Verdict::Pass) {
            Verdict::Pass
        } else {
            Verdict::Fail
        }
    }

    /// 전부 Pass로 초기화
    pub fn all_pass() -> Self {
        Self {
            loss_rate: Verdict::Pass,
            jitter: Verdict::Pass,
            out_of_order: Verdict::Pass,
            connection: Verdict::Pass,
        }
    }

    /// 실패 차원 목록 (MVS detail 벡터 스타일)
    pub fn failed_dimensions(&self) -> Vec<&'static str> {
        let mut failed = Vec::new();
        if self.loss_rate == Verdict::Fail { failed.push("loss_rate"); }
        if self.jitter == Verdict::Fail { failed.push("jitter"); }
        if self.out_of_order == Verdict::Fail { failed.push("out_of_order"); }
        if self.connection == Verdict::Fail { failed.push("connection"); }
        failed
    }

    /// MVS 스타일 한 줄 요약 (PPPP = all pass, FPPP = loss fail)
    pub fn summary_code(&self) -> String {
        let c = |v: Verdict| if v == Verdict::Pass { 'P' } else { 'F' };
        format!("{}{}{}{}", c(self.loss_rate), c(self.jitter), c(self.out_of_order), c(self.connection))
    }
}

// ============================================================================
// ParticipantVerdict — 참가자별 판정 결과
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParticipantVerdict {
    pub id: String,
    pub detail: DetailVerdict,
    /// 사유 (실패 시 구체적 수치)
    pub reasons: Vec<String>,
    /// 측정된 수치
    pub metrics: ParticipantMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParticipantMetrics {
    pub total_packets: u64,
    pub total_lost: u64,
    pub loss_rate_percent: f64,
    pub max_jitter: f64,
    pub total_ooo: u64,
}

// ============================================================================
// JudgeReport — 시나리오 판정 리포트
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JudgeReport {
    pub scenario: String,
    pub timestamp: String,
    pub verdict: Verdict,
    pub summary: String,
    pub participants: Vec<ParticipantVerdict>,
    pub errors: Vec<String>,
}

impl JudgeReport {
    /// JSON 직렬화
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_default()
    }

    /// 콘솔 출력용 요약
    pub fn print_summary(&self) {
        let icon = if self.verdict == Verdict::Pass { "✓" } else { "✗" };
        println!("\n═══ VERDICT: {} {} ═══", icon, self.verdict);
        println!("  scenario: {}", self.scenario);
        println!("  summary: {}", self.summary);
        for pv in &self.participants {
            let code = pv.detail.summary_code();
            let pf = pv.detail.overall();
            println!("  [{}] {} {} | rx={} lost={} loss={:.2}% jitter={:.0} ooo={}",
                pv.id, code, pf,
                pv.metrics.total_packets, pv.metrics.total_lost,
                pv.metrics.loss_rate_percent, pv.metrics.max_jitter, pv.metrics.total_ooo);
            for r in &pv.reasons {
                println!("    → {}", r);
            }
        }
        if !self.errors.is_empty() {
            println!("  errors: {}", self.errors.len());
        }
    }
}

// ============================================================================
// RecvInput — 봇 메트릭 입력 (oxlab-bot 의존 없이 사용 가능)
// ============================================================================

/// 판정기 입력용 수신 메트릭 (oxlab-bot::RecvSnapshot과 1:1 대응)
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
// judge() — 핵심 판정 함수
// ============================================================================

/// 시나리오 실행 결과를 판정
///
/// - participants: (id, RecvInput) 쌍
/// - errors: 시나리오 실행 중 발생한 에러
/// - thresholds: 판정 기준
pub fn judge(
    scenario_name: &str,
    participants: &HashMap<String, RecvInput>,
    errors: &[String],
    thresholds: &Thresholds,
) -> JudgeReport {
    let timestamp = chrono_now();
    let mut participant_verdicts = Vec::new();
    let mut all_pass = true;
    let mut fail_reasons = Vec::new();

    for (id, recv) in participants {
        let mut detail = DetailVerdict::all_pass();
        let mut reasons = Vec::new();

        // 1) Loss rate
        let total = recv.total_packets + recv.total_lost;
        let loss_pct = if total > 0 {
            (recv.total_lost as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        if loss_pct > thresholds.max_loss_rate_percent {
            detail.loss_rate = Verdict::Fail;
            reasons.push(format!("loss {:.2}% > {:.1}% threshold",
                loss_pct, thresholds.max_loss_rate_percent));
        }

        // 2) Jitter (max across streams)
        let max_jitter = recv.streams.iter()
            .map(|s| s.jitter)
            .fold(0.0f64, f64::max);
        if max_jitter > thresholds.max_jitter {
            detail.jitter = Verdict::Fail;
            reasons.push(format!("jitter {:.0} > {:.0} threshold",
                max_jitter, thresholds.max_jitter));
        }

        // 3) Out of order
        if recv.total_ooo > thresholds.max_ooo_count {
            detail.out_of_order = Verdict::Fail;
            reasons.push(format!("ooo {} > {} threshold",
                recv.total_ooo, thresholds.max_ooo_count));
        }

        // 4) Connection (에러 없으면 pass — 전체 레벨에서 체크)
        // 개별 참가자 connection은 일단 pass

        let metrics = ParticipantMetrics {
            total_packets: recv.total_packets,
            total_lost: recv.total_lost,
            loss_rate_percent: loss_pct,
            max_jitter,
            total_ooo: recv.total_ooo,
        };

        if detail.overall() == Verdict::Fail {
            all_pass = false;
            fail_reasons.push(format!("{}: {}", id, reasons.join(", ")));
        }

        participant_verdicts.push(ParticipantVerdict {
            id: id.clone(),
            detail,
            reasons,
            metrics,
        });
    }

    // Connection 에러 체크 (시나리오 레벨)
    if !errors.is_empty() {
        all_pass = false;
        fail_reasons.push(format!("{} scenario errors", errors.len()));
        // 모든 참가자에 connection fail 표시
        for pv in &mut participant_verdicts {
            pv.detail.connection = Verdict::Fail;
            pv.reasons.push(format!("{} scenario errors", errors.len()));
        }
    }

    // 참가자별 정렬 (id 순)
    participant_verdicts.sort_by(|a, b| a.id.cmp(&b.id));

    let verdict = if all_pass { Verdict::Pass } else { Verdict::Fail };
    let summary = if all_pass {
        "all checks passed".to_string()
    } else {
        fail_reasons.join("; ")
    };

    JudgeReport {
        scenario: scenario_name.to_string(),
        timestamp,
        verdict,
        summary,
        participants: participant_verdicts,
        errors: errors.to_vec(),
    }
}

fn chrono_now() -> String {
    // 간단한 ISO 타임스탬프 (chrono 의존 없이)
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{}s", d.as_secs())
}
