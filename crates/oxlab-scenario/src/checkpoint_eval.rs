// author: kodeholic (powered by Claude)
//! checkpoint_eval — Layer 1 체크포인트 평가
//!
//! 시나리오 실행 결과(recv_snapshots)를 기반으로 L1 체크포인트를 평가한다.
//! 평가 가능한 체크포인트만 실행하고, 나머지는 skip.

use std::collections::HashMap;

use oxlab_bot::RecvSnapshot;
use oxlab_judge::{CheckpointCategory, CheckpointResult, checkpoint_registry};

/// 시나리오 실행 결과로부터 Layer 1 체크포인트를 평가
///
/// - recv_snapshots: 참가자별 수신 메트릭
/// - applicable_categories: 이 시나리오에 적용할 카테고리 (비어있으면 전체)
/// - mode: 시나리오 모드 ("conference" | "ptt")
/// - profile: 네트워크 프로파일 ("pristine" 일 때만 일부 체크포인트 적용)
pub fn evaluate(
    recv_snapshots: &HashMap<String, RecvSnapshot>,
    applicable_categories: &[CheckpointCategory],
    mode: &str,
    profile: &str,
) -> Vec<CheckpointResult> {
    let mut results = Vec::new();

    // 적용할 체크포인트 필터링
    let defs = if applicable_categories.is_empty() {
        checkpoint_registry::REGISTRY.iter().collect::<Vec<_>>()
    } else {
        checkpoint_registry::find_by_categories(applicable_categories)
    };

    for def in &defs {
        // 각 체크포인트별 평가 함수 dispatch
        let result = match def.id {
            "L1-13" => eval_l1_13_fanout_integrity(recv_snapshots, def, profile),
            // TODO: 봇 관측 포인트 추가 후 활성화
            // "L1-01" => eval_l1_01(...)
            // "L1-04" => eval_l1_04(...)
            _ => None, // 아직 평가 불가 → skip
        };

        if let Some(r) = result {
            results.push(r);
        }
    }

    results
}

/// L1-13: fan-out integrity (pristine)
///
/// pristine 환경에서:
/// 1) 모든 subscriber의 loss가 0이어야 한다
/// 2) cross-subscriber: 같은 SSRC에 대해 subscriber 간 수신 패킷 수 차이 ≤ 2
fn eval_l1_13_fanout_integrity(
    recv_snapshots: &HashMap<String, RecvSnapshot>,
    def: &checkpoint_registry::CheckpointDef,
    profile: &str,
) -> Option<CheckpointResult> {
    // pristine이 아니면 skip (네트워크 loss와 SFU 버그를 구분 못 함)
    if profile != "pristine" {
        return None;
    }

    if recv_snapshots.is_empty() {
        return Some(CheckpointResult::fail_from_def(def, "no recv data"));
    }

    // 1) 모든 subscriber의 loss가 0인지 확인
    let mut total_lost_all: u64 = 0;
    let mut loss_details = Vec::new();

    for (id, snap) in recv_snapshots {
        if snap.total_lost > 0 {
            total_lost_all += snap.total_lost;
            loss_details.push(format!("{}:lost={}", id, snap.total_lost));
        }
    }

    if total_lost_all > 0 {
        return Some(CheckpointResult::fail_from_def(
            def,
            &format!("total_lost={} [{}]", total_lost_all, loss_details.join(", ")),
        ));
    }

    // 2) cross-subscriber: 같은 SSRC를 수신한 subscriber들의 패킷 수 비교
    //    SSRC → [(bot_id, packets_received)]
    let mut ssrc_receivers: HashMap<u32, Vec<(&str, u64)>> = HashMap::new();
    for (id, snap) in recv_snapshots {
        for stream in &snap.streams {
            ssrc_receivers
                .entry(stream.ssrc)
                .or_default()
                .push((id.as_str(), stream.packets_received));
        }
    }

    let mut mismatch_details = Vec::new();
    for (ssrc, receivers) in &ssrc_receivers {
        if receivers.len() < 2 { continue; }

        let max_rx = receivers.iter().map(|(_, c)| *c).max().unwrap_or(0);
        let min_rx = receivers.iter().map(|(_, c)| *c).min().unwrap_or(0);

        // 시작/종료 타이밍 차이 허용 (2패킷)
        if max_rx.saturating_sub(min_rx) > 2 {
            let detail: Vec<String> = receivers.iter()
                .map(|(id, c)| format!("{}={}", id, c))
                .collect();
            mismatch_details.push(
                format!("SSRC=0x{:08X} Δ={} [{}]", ssrc, max_rx - min_rx, detail.join(","))
            );
        }
    }

    if !mismatch_details.is_empty() {
        return Some(CheckpointResult::fail_from_def(
            def,
            &format!("cross-subscriber mismatch: {}", mismatch_details.join("; ")),
        ));
    }

    let total_rx: u64 = recv_snapshots.values().map(|s| s.total_packets).sum();
    Some(CheckpointResult::pass_from_def(
        def,
        &format!("total_rx={} lost=0 cross-check=OK ({} subscribers)",
            total_rx, recv_snapshots.len()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_snap(total_packets: u64, total_lost: u64) -> RecvSnapshot {
        RecvSnapshot {
            total_packets,
            total_lost,
            total_ooo: 0,
            ssrc_count: 0,
            streams: vec![],
        }
    }

    #[test]
    fn l1_13_pristine_no_loss() {
        let mut snaps = HashMap::new();
        snaps.insert("bot_1".into(), empty_snap(100, 0));
        snaps.insert("bot_2".into(), empty_snap(100, 0));

        let cats = vec![CheckpointCategory::CoreRelay];
        let results = evaluate(&snaps, &cats, "conference", "pristine");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "L1-13");
        assert_eq!(results[0].verdict, oxlab_judge::Verdict::Pass);
    }

    #[test]
    fn l1_13_pristine_with_loss_fails() {
        let mut snaps = HashMap::new();
        snaps.insert("bot_1".into(), empty_snap(100, 3));
        snaps.insert("bot_2".into(), empty_snap(100, 0));

        let cats = vec![CheckpointCategory::CoreRelay];
        let results = evaluate(&snaps, &cats, "conference", "pristine");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].verdict, oxlab_judge::Verdict::Fail);
        assert!(results[0].actual.contains("total_lost=3"));
    }

    #[test]
    fn l1_13_non_pristine_skipped() {
        let mut snaps = HashMap::new();
        snaps.insert("bot_1".into(), empty_snap(100, 5));

        let cats = vec![CheckpointCategory::CoreRelay];
        let results = evaluate(&snaps, &cats, "conference", "field_lte");
        // non-pristine → skip
        assert!(results.is_empty());
    }

    #[test]
    fn l1_13_empty_snapshots_fails() {
        let snaps = HashMap::new();
        let cats = vec![CheckpointCategory::CoreRelay];
        let results = evaluate(&snaps, &cats, "conference", "pristine");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].verdict, oxlab_judge::Verdict::Fail);
    }

    #[test]
    fn ptt_categories_skip_l1_13() {
        let mut snaps = HashMap::new();
        snaps.insert("bot_1".into(), empty_snap(100, 0));

        // PttRelay + FloorControl에 L1-13(CoreRelay)은 포함 안 됨
        let cats = vec![CheckpointCategory::PttRelay, CheckpointCategory::FloorControl];
        let results = evaluate(&snaps, &cats, "ptt", "pristine");
        // L1-13은 CoreRelay 카테고리이므로 skip
        assert!(results.iter().all(|r| r.id != "L1-13"));
    }
}
