// author: kodeholic (powered by Claude)
//! Checkpoint Registry — Layer 1 SFU 행동 검증 체크포인트 정의
//!
//! L1-01 ~ L1-21 static 레지스트리.
//! 기능 추가 시 여기에 체크포인트를 누적 등록한다.

use crate::CheckpointCategory;

/// 체크포인트 정의 (static 레지스트리 항목)
pub struct CheckpointDef {
    pub id: &'static str,
    pub name: &'static str,
    pub category: CheckpointCategory,
    pub description: &'static str,
}

/// 전체 레지스트리 (L1-01 ~ L1-21)
pub static REGISTRY: &[CheckpointDef] = &[
    // ── RTCP Terminator ──
    CheckpointDef {
        id: "L1-01",
        name: "subscriber RR relay blocked",
        category: CheckpointCategory::RtcpTerminator,
        description: "subscriber의 RR이 publisher에게 릴레이되지 않아야 한다",
    },

    // ── SR Translation ──
    CheckpointDef {
        id: "L1-02",
        name: "SR NTP timestamp preserved",
        category: CheckpointCategory::SrTranslation,
        description: "SR의 NTP timestamp은 publisher 원본 그대로 유지되어야 한다",
    },
    CheckpointDef {
        id: "L1-03",
        name: "SR RTP ts continuity",
        category: CheckpointCategory::SrTranslation,
        description: "SR의 RTP timestamp은 구독 레이어 offset과 일관되어야 한다",
    },

    // ── PTT Relay ──
    CheckpointDef {
        id: "L1-04",
        name: "PTT non-speaker RTP gating",
        category: CheckpointCategory::PttRelay,
        description: "비발화자의 RTP는 ingress에서 차단되어야 한다",
    },
    CheckpointDef {
        id: "L1-05",
        name: "PTT silence flush",
        category: CheckpointCategory::PttRelay,
        description: "clear_speaker 시 Opus silence 3프레임이 주입되어야 한다",
    },
    CheckpointDef {
        id: "L1-06",
        name: "PTT speaker switch keyframe first",
        category: CheckpointCategory::PttRelay,
        description: "화자 전환 후 첫 릴레이 패킷은 VP8 keyframe이어야 한다",
    },
    CheckpointDef {
        id: "L1-07",
        name: "SSRC rewriting consistency",
        category: CheckpointCategory::PttRelay,
        description: "virtual SSRC가 화자 전환 후에도 일관되어야 한다",
    },
    CheckpointDef {
        id: "L1-08",
        name: "ts_gap continuity after idle",
        category: CheckpointCategory::PttRelay,
        description: "idle 후 복귀 시 ts gap이 경과 시간을 반영해야 한다",
    },

    // ── PLI Governor ──
    CheckpointDef {
        id: "L1-09",
        name: "PLI → keyframe response",
        category: CheckpointCategory::PliGovernor,
        description: "PLI 전송 후 제한 시간 내에 keyframe이 도착해야 한다",
    },
    CheckpointDef {
        id: "L1-10",
        name: "PLI burst auto-cancel on keyframe",
        category: CheckpointCategory::PliGovernor,
        description: "keyframe 도착 시 잔여 PLI burst가 취소되어야 한다",
    },

    // ── SubscriberGate ──
    CheckpointDef {
        id: "L1-11",
        name: "SubscriberGate blocks before ACK",
        category: CheckpointCategory::SubscriberGate,
        description: "TRACKS_ACK 수신 전에는 video 패킷이 0개여야 한다",
    },
    CheckpointDef {
        id: "L1-12",
        name: "SubscriberGate GATE:PLI after ACK",
        category: CheckpointCategory::SubscriberGate,
        description: "TRACKS_ACK 수신 후 GATE:PLI가 발사되어야 한다",
    },

    // ── Core Relay ──
    CheckpointDef {
        id: "L1-13",
        name: "fan-out integrity (pristine)",
        category: CheckpointCategory::CoreRelay,
        description: "pristine 환경에서 전원 동일 seq 수신 (무손실 fan-out)",
    },

    // ── Floor Control ──
    CheckpointDef {
        id: "L1-14",
        name: "floor grant priority order",
        category: CheckpointCategory::FloorControl,
        description: "floor_request는 priority 순으로 grant되어야 한다",
    },
    CheckpointDef {
        id: "L1-15",
        name: "preemption → revoke delivery",
        category: CheckpointCategory::FloorControl,
        description: "preemption 발생 시 현 발화자에게 revoke가 도착해야 한다",
    },
    CheckpointDef {
        id: "L1-16",
        name: "queue position consistency",
        category: CheckpointCategory::FloorControl,
        description: "큐 위치 응답이 실제 큐 순서와 일치해야 한다",
    },

    // ── Lifecycle ──
    CheckpointDef {
        id: "L1-17",
        name: "zombie cleanup",
        category: CheckpointCategory::Lifecycle,
        description: "비정상 종료 후 제한 시간 내에 cleanup되어야 한다",
    },

    // ── Simulcast ──
    CheckpointDef {
        id: "L1-18",
        name: "simulcast layer switch SSRC",
        category: CheckpointCategory::Simulcast,
        description: "레이어 전환 시 새 레이어 SSRC로 전환되어야 한다",
    },
    CheckpointDef {
        id: "L1-19",
        name: "simulcast layer switch ts continuity",
        category: CheckpointCategory::Simulcast,
        description: "레이어 전환 후 ts offset이 유지되어야 한다",
    },
    CheckpointDef {
        id: "L1-20",
        name: "simulcast layer switch keyframe first",
        category: CheckpointCategory::Simulcast,
        description: "레이어 전환 시 I-frame이 선행해야 한다",
    },

    // ── Screen Share ──
    CheckpointDef {
        id: "L1-21",
        name: "screen share non-simulcast relay",
        category: CheckpointCategory::ScreenShare,
        description: "screen share는 원본 SSRC 유지 + non-simulcast relay",
    },
];

/// ID로 체크포인트 정의 검색
pub fn find_by_id(id: &str) -> Option<&'static CheckpointDef> {
    REGISTRY.iter().find(|d| d.id == id)
}

/// 카테고리로 필터링
pub fn find_by_category(category: CheckpointCategory) -> Vec<&'static CheckpointDef> {
    REGISTRY.iter().filter(|d| d.category == category).collect()
}

/// 복수 카테고리로 필터링
pub fn find_by_categories(categories: &[CheckpointCategory]) -> Vec<&'static CheckpointDef> {
    REGISTRY.iter()
        .filter(|d| categories.contains(&d.category))
        .collect()
}

/// 전체 ID 목록
pub fn all_ids() -> Vec<&'static str> {
    REGISTRY.iter().map(|d| d.id).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_has_21_entries() {
        assert_eq!(REGISTRY.len(), 21);
    }

    #[test]
    fn find_by_id_works() {
        let cp = find_by_id("L1-04").unwrap();
        assert_eq!(cp.name, "PTT non-speaker RTP gating");
        assert_eq!(cp.category, CheckpointCategory::PttRelay);
    }

    #[test]
    fn find_by_category_works() {
        let ptt = find_by_category(CheckpointCategory::PttRelay);
        assert_eq!(ptt.len(), 5); // L1-04 ~ L1-08
    }

    #[test]
    fn find_by_id_missing() {
        assert!(find_by_id("L1-99").is_none());
    }
}
