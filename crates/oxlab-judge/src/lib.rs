// author: kodeholic (powered by Claude)
//! oxlab-judge — 판정기
//!
//! Phase 0: Verdict enum + 스켈레톤
//! Phase 2 예정: 메트릭 수집 + 임계치 판정 + JSON 리포트
//!
//! MVS의 16자리 detail 벡터에서 영감.
//! 단순 pass/fail이 아니라 실패 차원을 다차원으로 분류.

use serde::{Deserialize, Serialize};

/// 최종 판정
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    Pass,
    Fail,
}

/// 차원별 상세 판정 (MVS의 detail[16] 대응)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailVerdict {
    pub video_freeze: Verdict,
    pub audio_gap: Verdict,
    pub loss_rate: Verdict,
    pub jb_delay: Verdict,
    pub floor_latency: Verdict,
    pub speaker_switch: Verdict,
    pub contract_rr: Verdict,
    pub contract_sr: Verdict,
    pub sequence: Verdict,
    pub connection: Verdict,
}

impl DetailVerdict {
    /// 모든 차원이 Pass면 Pass
    pub fn overall(&self) -> Verdict {
        let all = [
            self.video_freeze,
            self.audio_gap,
            self.loss_rate,
            self.jb_delay,
            self.floor_latency,
            self.speaker_switch,
            self.contract_rr,
            self.contract_sr,
            self.sequence,
            self.connection,
        ];
        if all.iter().all(|v| *v == Verdict::Pass) {
            Verdict::Pass
        } else {
            Verdict::Fail
        }
    }

    /// 전부 Pass로 초기화 (MVS의 "PPPPPPPPPPPPPPPP")
    pub fn all_pass() -> Self {
        Self {
            video_freeze: Verdict::Pass,
            audio_gap: Verdict::Pass,
            loss_rate: Verdict::Pass,
            jb_delay: Verdict::Pass,
            floor_latency: Verdict::Pass,
            speaker_switch: Verdict::Pass,
            contract_rr: Verdict::Pass,
            contract_sr: Verdict::Pass,
            sequence: Verdict::Pass,
            connection: Verdict::Pass,
        }
    }
}

/// 판정 실행 (Phase 2에서 구현)
pub fn judge() -> DetailVerdict {
    tracing::warn!("oxlab-judge::judge() — not yet implemented (Phase 2)");
    DetailVerdict::all_pass()
}
