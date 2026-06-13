// author: kodeholic (powered by Claude)
//! 분산 capacity — coordinator + worker 프로세스.
//!
//! 설계: `context/design/20260613_capacity_test_design.md` §2 "분산 봇 — 복호-skip 으로 단일
//! 머신 천장 끌어올린 뒤 부족하면". 단일 머신 DTLS setup 천장(~200대)을 넘어 SFU 진짜 천장을 본다.
//!
//! 구조:
//! - **worker** (`oxlab cap-worker`): 지정 방에 sub 봇 M 개 setup → `--start-at-ms` 시각까지 대기
//!   → duration 측정(counters delta) → JSON report stdout. 여러 프로세스/머신(ssh)으로 확장.
//! - **coordinator** (`oxlab cap-dist`): publisher 1 + worker K 프로세스 spawn(self exe 재실행).
//!   절대 시각(start_at) 으로 발행·측정 동기(파일 IPC 불요). worker report 합산 → 곡선 1점.
//!
//! 동기: SystemTime 절대 시각(start_at_ms). 같은 머신 = 정확, 멀티머신 = NTP 가정.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use oxlab_bot::{run_publisher, run_subscriber, CapCounters, RecvMode};
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tokio::sync::{watch, Mutex, Semaphore};
use tracing::{info, warn};

use crate::capacity::sample_resources;

/// worker → coordinator 결과 (JSON, stdout 한 줄).
#[derive(Serialize, Deserialize, Default, Debug)]
pub struct WorkerReport {
    pub setup: usize,
    pub active: usize,
    pub rx_packets: u64,
    pub rx_lost: u64,
    pub rx_bytes: u64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 절대 시각(epoch ms)까지 대기.
async fn wait_until(target_ms: u64) {
    let now = now_ms();
    if target_ms > now {
        tokio::time::sleep(Duration::from_millis(target_ms - now)).await;
    }
}

// ════════════════════════════ worker ════════════════════════════

/// worker — sub 봇 M 개 setup → start_at 대기 → duration 측정(delta) → JSON report.
#[allow(clippy::too_many_arguments)]
pub async fn run_cap_worker(
    server: String,
    ws_port: u16,
    room_id: String,
    count: usize,
    full_count: usize,
    duration_secs: u64,
    start_at_ms: u64,
    wid: usize,
) {
    let counters = CapCounters::new();
    let lat: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::new()));
    let ready = Arc::new(AtomicUsize::new(0));
    let setup_sem = Arc::new(Semaphore::new(32));
    // run_window = start_at 대기 + 측정 + 여유 (봇이 측정 끝까지 살아있게).
    let now = now_ms();
    let wait_secs = start_at_ms.saturating_sub(now) / 1000;
    let run_window = Duration::from_secs(wait_secs + duration_secs + 15);

    let mut handles = Vec::new();
    for i in 0..count {
        let recv_mode = if i < full_count { RecvMode::Full } else { RecvMode::Count };
        handles.push(tokio::spawn(run_subscriber(
            format!("w{wid}-sub-{i}"),
            server.clone(),
            ws_port,
            room_id.clone(),
            recv_mode,
            run_window,
            Arc::clone(&counters),
            Arc::clone(&lat),
            Arc::clone(&setup_sem),
            Arc::clone(&ready),
        )));
    }

    // start_at 대기 → measure 윈도우 delta 측정 (publisher 발행 정렬)
    wait_until(start_at_ms).await;
    let before = (
        counters.rx_packets.load(Ordering::Relaxed),
        counters.rx_lost.load(Ordering::Relaxed),
        counters.rx_bytes.load(Ordering::Relaxed),
    );
    tokio::time::sleep(Duration::from_secs(duration_secs)).await;
    let after = (
        counters.rx_packets.load(Ordering::Relaxed),
        counters.rx_lost.load(Ordering::Relaxed),
        counters.rx_bytes.load(Ordering::Relaxed),
    );

    for h in handles {
        h.abort();
    }

    let report = WorkerReport {
        setup: ready.load(Ordering::Relaxed),
        active: counters.active_subs.load(Ordering::Relaxed),
        rx_packets: after.0.saturating_sub(before.0),
        rx_lost: after.1.saturating_sub(before.1),
        rx_bytes: after.2.saturating_sub(before.2),
    };
    // stdout 한 줄 = coordinator 가 파싱. (로그는 stderr)
    println!("{}", serde_json::to_string(&report).unwrap_or_default());
}

// ════════════════════════════ coordinator ════════════════════════════

/// coordinator — publisher 1 + worker K 프로세스. 분산 sub 합산으로 1점 측정.
#[allow(clippy::too_many_arguments)]
pub async fn run_distributed(
    server: String,
    ws_port: u16,
    total: usize,
    workers: usize,
    duration_secs: u64,
    full_count: usize,
    ts: u64,
) {
    let room_id = format!("cap-dist-{total}-{ts}");
    let per_worker = total.div_ceil(workers.max(1));
    let duration = Duration::from_secs(duration_secs);

    info!(
        "=== 분산 capacity: total={} workers={} (per~{}) duration={}s ===",
        total, workers, per_worker, duration_secs
    );

    // setup_grace: publisher + worker 전원 setup 여유 (total 비례). start_at 까지.
    let setup_grace_ms = 8_000 + (total as u64) * 20;
    let start_at_ms = now_ms() + setup_grace_ms;

    // ── publisher (coordinator 프로세스 내, trigger=start_at) ──
    let pubc = CapCounters::new();
    let (trig_tx, trig_rx) = watch::channel(false);
    let pub_handle = tokio::spawn(run_publisher(
        "dist-pub".to_string(),
        server.clone(),
        ws_port,
        room_id.clone(),
        "full",
        duration,
        Arc::clone(&pubc),
        trig_rx,
    ));

    // publisher 가 방 먼저 생성하도록 잠깐 양보 후 worker spawn
    tokio::time::sleep(Duration::from_secs(1)).await;

    // ── worker K 프로세스 spawn (self exe 재실행) ──
    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => {
            warn!("current_exe failed: {e}");
            return;
        }
    };
    let mut children = Vec::new();
    for w in 0..workers {
        // 마지막 worker 가 나머지 흡수
        let cnt = if w == workers - 1 {
            total - per_worker * (workers - 1)
        } else {
            per_worker
        };
        if cnt == 0 {
            continue;
        }
        let full = if w == 0 { full_count } else { 0 }; // Full 봇은 worker 0 에 집중
        let child = Command::new(&exe)
            .args([
                "cap-worker",
                "--server", &server,
                "--port", &ws_port.to_string(),
                "--room", &room_id,
                "--count", &cnt.to_string(),
                "--full", &full.to_string(),
                "--duration", &duration_secs.to_string(),
                "--start-at-ms", &start_at_ms.to_string(),
                "--wid", &w.to_string(),
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn();
        match child {
            Ok(c) => children.push(c),
            Err(e) => warn!("worker {w} spawn failed: {e}"),
        }
    }

    // ── 자원 샘플러 + start_at 발행 동기 ──
    let (stop_tx, stop_rx) = watch::channel(false);
    let sampler = tokio::spawn(sample_resources(stop_rx));

    wait_until(start_at_ms).await;
    let _ = trig_tx.send(true); // publisher 발행 (worker 측정 윈도우와 동일 시각)
    let t0 = std::time::Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let _ = stop_tx.send(true);
    let res = sampler.await.unwrap_or_default();

    // ── worker report 수집 ──
    let mut total_setup = 0usize;
    let mut total_active = 0usize;
    let mut total_rx = 0u64;
    let mut total_lost = 0u64;
    let mut total_bytes = 0u64;
    for child in children {
        match child.wait_with_output().await {
            Ok(out) => {
                let s = String::from_utf8_lossy(&out.stdout);
                let line = s.lines().last().unwrap_or("");
                match serde_json::from_str::<WorkerReport>(line) {
                    Ok(r) => {
                        total_setup += r.setup;
                        total_active += r.active;
                        total_rx += r.rx_packets;
                        total_lost += r.rx_lost;
                        total_bytes += r.rx_bytes;
                    }
                    Err(e) => warn!("worker report parse fail: {e} line='{line}'"),
                }
            }
            Err(e) => warn!("worker wait fail: {e}"),
        }
    }
    let _ = pub_handle.await;

    // ── 집계 ──
    let tx_ok = pubc.tx_ok.load(Ordering::Relaxed);
    let in_pps = tx_ok as f64 / elapsed;
    let out_pps = total_rx as f64 / elapsed;
    let out_mbps = (total_bytes as f64 * 8.0) / (elapsed * 1_000_000.0);
    let loss_pct = if total_rx + total_lost > 0 {
        total_lost as f64 / (total_rx + total_lost) as f64 * 100.0
    } else {
        0.0
    };
    let subs_ok = total == 0 || total_active * 10 >= total * 9;
    let cpu_headroom = res.self_cpu_avg < (res.cores as f64 * 90.0);
    let bot_healthy = subs_ok && cpu_headroom;

    println!();
    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║  분산 CAPACITY — total={:<5} workers={:<3}                              ", total, workers);
    println!("╚══════════════════════════════════════════════════════════════════════╝");
    println!(
        "  setup={}/{}  active={}/{}  in_pps={:.0}  out_pps={:.0}  out_mbps={:.2}  loss={:.3}%",
        total_setup, total, total_active, total, in_pps, out_pps, out_mbps, loss_pct
    );
    println!(
        "  sfu_cpu={:.0}%  sfu_mem={:.0}MB  coord_cpu={:.0}%  healthy={}",
        res.sfu_cpu_avg, res.sfu_mem_mb, res.self_cpu_avg,
        if bot_healthy { "y" } else { "n" }
    );
    println!();
}
