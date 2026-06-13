// author: kodeholic (powered by Claude)
//! Capacity 측정 runner — N 스윕 부하 천장.
//!
//! 설계: `context/design/20260613_capacity_test_design.md` §6 모드 / §7 스윕·출력 / §8 병목 분리.
//!
//! 각 N: publisher 1 + sub N (N-full / full 봇은 전수복호). setup → trigger → measure →
//! teardown → row. 출력 표 + `reports/cap_{mode}_{ts}.csv`. knee 는 육안(자동 판정 안 함, §7).
//!
//! bot_healthy(§8): publisher tx_ok/tx_attempt. unhealthy row = 봇 천장(SFU 천장 아님 — 폐기 후보).
//! admin SfuMetrics 교차(§8)는 현 서버 REST `/media/admin/metrics` 가 stub("not implemented")이라
//! 이번 단계는 봇측 카운터로 갈음 — 완료보고에 한계 명시(향후 admin WS ADMIN_METRICS 스트림 확장).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use oxlab_bot::{run_conf_bot, run_publisher, run_subscriber, CapCounters, RecvMode};
use sysinfo::{Pid, ProcessesToUpdate, System};
use tokio::sync::{watch, Mutex, Semaphore};
use tokio::time::{interval, Instant};
use tracing::info;

/// measure 윈도우 동안의 자원 사용 (봇 self + sfud 프로세스).
#[derive(Default, Clone, Copy)]
pub(crate) struct ResStats {
    pub self_cpu_avg: f64,  // 봇 프로세스 CPU% (코어당 100 합산)
    pub self_mem_mb: f64,   // 봇 프로세스 RSS peak (MB)
    pub sfu_cpu_avg: f64,   // oxsfud 합산 CPU%
    pub sfu_mem_mb: f64,    // oxsfud 합산 RSS peak (MB)
    pub cores: usize,       // 논리 코어 수 (봇 CPU 천장 = cores×100)
}

/// measure 동안 1초 간격으로 봇 self + sfud CPU/mem 샘플링. stop 신호까지.
pub(crate) async fn sample_resources(mut stop: watch::Receiver<bool>) -> ResStats {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let self_pid = Pid::from_u32(std::process::id());
    let mut sys = System::new();
    // baseline refresh (cpu_usage 는 직전 refresh 대비라 첫 값은 0)
    sys.refresh_processes(ProcessesToUpdate::All, true);

    let mut self_cpu: Vec<f32> = Vec::new();
    let mut self_mem_peak: u64 = 0;
    let mut sfu_cpu: Vec<f32> = Vec::new();
    let mut sfu_mem_peak: u64 = 0;

    let mut tick = interval(Duration::from_secs(1));
    tick.tick().await; // 즉발 1회 소비

    loop {
        tokio::select! {
            r = stop.changed() => {
                if r.is_err() || *stop.borrow() { break; }
            }
            _ = tick.tick() => {
                sys.refresh_processes(ProcessesToUpdate::All, true);
                if let Some(p) = sys.process(self_pid) {
                    self_cpu.push(p.cpu_usage());
                    self_mem_peak = self_mem_peak.max(p.memory());
                }
                let mut c = 0f32;
                let mut m = 0u64;
                for p in sys.processes().values() {
                    if p.name().to_string_lossy().contains("oxsfud") {
                        c += p.cpu_usage();
                        m += p.memory();
                    }
                }
                sfu_cpu.push(c);
                sfu_mem_peak = sfu_mem_peak.max(m);
            }
        }
    }

    let avg = |v: &[f32]| -> f64 {
        if v.is_empty() { 0.0 } else { v.iter().map(|&x| x as f64).sum::<f64>() / v.len() as f64 }
    };
    ResStats {
        self_cpu_avg: avg(&self_cpu),
        self_mem_mb: self_mem_peak as f64 / 1_000_000.0,
        sfu_cpu_avg: avg(&sfu_cpu),
        sfu_mem_mb: sfu_mem_peak as f64 / 1_000_000.0,
        cores,
    }
}

/// 한 N 측정 결과 (표/CSV 한 행).
struct Row {
    n: usize,
    active: usize,
    in_pps: f64,
    out_pps: f64,
    out_mbps: f64,
    loss_pct: f64,
    lat_avg: f64,
    lat_p95: f64,
    lat_max: f64,
    self_cpu: f64,
    self_mem_mb: f64,
    sfu_cpu: f64,
    sfu_mem_mb: f64,
    bot_healthy: bool,
}

/// `oxlab cap` 진입점. mode = "broadcast" | "ptt". ts = 호출자 생성 timestamp(ms).
pub async fn run_capacity(
    server: String,
    ws_port: u16,
    mode: String,
    sweep: Vec<usize>,
    duration_secs: u64,
    full_count: usize,
    ts: u64,
) {
    let duplex: &'static str = match mode.as_str() {
        "ptt" => "half",
        _ => "full",
    };
    info!(
        "=== Capacity 측정: mode={} duplex={} sweep={:?} duration={}s full={} ===",
        mode, duplex, sweep, duration_secs, full_count
    );

    let mut rows = Vec::new();
    for &n in &sweep {
        info!("── N={n} 측정 시작 ──");
        let row = if mode == "conf" {
            run_one_conf(&server, ws_port, n, duration_secs, full_count, ts).await
        } else {
            run_one(&server, ws_port, &mode, duplex, n, duration_secs, full_count, ts).await
        };
        print_row(&mode, &row);
        rows.push(row);
    }

    print_table(&mode, &rows);
    match write_csv(&mode, ts, &rows) {
        Ok(path) => info!("CSV written: {path}"),
        Err(e) => info!("CSV write failed: {e}"),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_one(
    server: &str,
    ws_port: u16,
    mode: &str,
    duplex: &'static str,
    n: usize,
    duration_secs: u64,
    full_count: usize,
    ts: u64,
) -> Row {
    let room_id = format!("cap-{mode}-{n}-{ts}");
    let duration = Duration::from_secs(duration_secs);
    let grace_room = Duration::from_secs(1);
    // ready-sync: 전원 setup 완료까지 대기 후 발행. 봇 STUN consent 로 reaper(idle) 무력화돼
    // 긴 setup 대기도 안전(이전 회귀는 consent 부재로 sub 가 zombie 된 것). N 비례 상한.
    let ready_cap = Duration::from_secs(20 + (n as u64) / 15);
    let margin = Duration::from_secs(2);
    let run_window = ready_cap + duration + margin;

    let pubc = CapCounters::new();
    let subc = CapCounters::new();
    let lat: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::new()));
    let ready = Arc::new(AtomicUsize::new(0));
    let (trig_tx, trig_rx) = watch::channel(false);

    let mut handles = Vec::new();

    // publisher (방 ensure + 발행)
    handles.push(tokio::spawn(run_publisher(
        format!("cap-pub-{n}"),
        server.to_string(),
        ws_port,
        room_id.clone(),
        duplex,
        duration,
        Arc::clone(&pubc),
        trig_rx.clone(),
    )));

    tokio::time::sleep(grace_room).await; // publisher 방 생성 + media

    // 동시 DTLS 핸드셰이크 제한 (storm 회피 — bench sequential 정신). 32 동시.
    let setup_sem = Arc::new(Semaphore::new(32));

    // subscribers (앞 full_count 대만 Full 복호, 나머지 Count)
    for i in 0..n {
        let recv_mode = if i < full_count { RecvMode::Full } else { RecvMode::Count };
        handles.push(tokio::spawn(run_subscriber(
            format!("cap-sub-{n}-{i}"),
            server.to_string(),
            ws_port,
            room_id.clone(),
            recv_mode,
            run_window,
            Arc::clone(&subc),
            Arc::clone(&lat),
            Arc::clone(&setup_sem),
            Arc::clone(&ready),
        )));
    }

    // ready-sync: sub 전원 setup 완료(또는 상한) 대기 후 발행 → 모든 sub 가 fan-out 받음.
    let ready_deadline = Instant::now() + ready_cap;
    loop {
        let r = ready.load(Ordering::Relaxed);
        if r >= n || Instant::now() >= ready_deadline {
            let tag = if r < n { " (상한 timeout — 일부 sub 미완)" } else { "" };
            info!("N={n}: setup ready {r}/{n}{tag} → 발행");
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // 자원 샘플러 시작 (measure 윈도우 = trigger ~ duration)
    let (stop_tx, stop_rx) = watch::channel(false);
    let sampler = tokio::spawn(sample_resources(stop_rx));

    let _ = trig_tx.send(true); // publisher 발행 시작 (notify_new_stream → 전 sub fan-out)
    let t0 = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();

    let _ = stop_tx.send(true);
    let res = sampler.await.unwrap_or_default();

    // 봇 task 종료 대기 (run_window deadline 자체 종료)
    for h in handles {
        let _ = h.await;
    }

    // ── 집계 ──
    use std::sync::atomic::Ordering::Relaxed;
    let tx_attempt = pubc.tx_attempt.load(Relaxed);
    let tx_ok = pubc.tx_ok.load(Relaxed);
    let out_pkts = subc.rx_packets.load(Relaxed);
    let lost = subc.rx_lost.load(Relaxed);
    let rx_bytes = subc.rx_bytes.load(Relaxed);
    let active = subc.active_subs.load(Relaxed);

    let in_pps = tx_ok as f64 / elapsed;
    let out_pps = out_pkts as f64 / elapsed;
    let out_mbps = (rx_bytes as f64 * 8.0) / (elapsed * 1_000_000.0);
    let loss_pct = if out_pkts + lost > 0 {
        lost as f64 / (out_pkts + lost) as f64 * 100.0
    } else {
        0.0
    };

    let mut lats = lat.lock().await.clone();
    lats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let (lat_avg, lat_p95, lat_max) = lat_stats(&lats);

    // bot_healthy(§8): 송신 천장(tx_ok/tx_attempt) AND 봇 CPU 여유 AND 활성 sub ≥ 90%.
    // 셋 중 하나라도 깨지면 측정 도구 천장/누락 → 해당 row 의 곡선값은 SFU 천장 아님(폐기 후보).
    let tx_healthy = tx_attempt == 0 || tx_ok * 100 >= tx_attempt * 99;
    let cpu_headroom = res.self_cpu_avg < (res.cores as f64 * 90.0);
    let subs_ok = n == 0 || active * 10 >= n * 9; // 90% sub 가 실제 fan-out 수신
    let bot_healthy = tx_healthy && cpu_headroom && subs_ok;

    Row {
        n,
        active,
        in_pps,
        out_pps,
        out_mbps,
        loss_pct,
        lat_avg,
        lat_p95,
        lat_max,
        self_cpu: res.self_cpu_avg,
        self_mem_mb: res.self_mem_mb,
        sfu_cpu: res.sfu_cpu_avg,
        sfu_mem_mb: res.sfu_mem_mb,
        bot_healthy,
    }
}

/// Conference 모드 — N명 전원 pub+sub (raw mesh). in/out 둘 다 전원 합산.
async fn run_one_conf(
    server: &str,
    ws_port: u16,
    n: usize,
    duration_secs: u64,
    full_count: usize,
    ts: u64,
) -> Row {
    let room_id = format!("cap-conf-{n}-{ts}");
    let duration = Duration::from_secs(duration_secs);
    let grace_room = Duration::from_secs(1);
    let ready_cap = Duration::from_secs(20 + (n as u64) / 15);

    let counters = CapCounters::new(); // conf: 전원 pub+sub 통합
    let lat: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::new()));
    let ready = Arc::new(AtomicUsize::new(0));
    let (trig_tx, trig_rx) = watch::channel(false);
    let setup_sem = Arc::new(Semaphore::new(32));

    let mut handles = Vec::new();
    // creator 먼저 (방 생성) → 나머지 join 가능
    handles.push(tokio::spawn(run_conf_bot(
        format!("cap-conf-{n}-0"), server.to_string(), ws_port, room_id.clone(),
        true, duration, RecvMode::Full,
        Arc::clone(&counters), Arc::clone(&lat), Arc::clone(&setup_sem),
        Arc::clone(&ready), trig_rx.clone(),
    )));
    tokio::time::sleep(grace_room).await;
    for i in 1..n {
        let recv_mode = if i < full_count { RecvMode::Full } else { RecvMode::Count };
        handles.push(tokio::spawn(run_conf_bot(
            format!("cap-conf-{n}-{i}"), server.to_string(), ws_port, room_id.clone(),
            false, duration, recv_mode,
            Arc::clone(&counters), Arc::clone(&lat), Arc::clone(&setup_sem),
            Arc::clone(&ready), trig_rx.clone(),
        )));
    }

    // ready-sync (전원 setup 후 동시 발행)
    let ready_deadline = Instant::now() + ready_cap;
    loop {
        let r = ready.load(Ordering::Relaxed);
        if r >= n || Instant::now() >= ready_deadline {
            let tag = if r < n { " (상한 timeout)" } else { "" };
            info!("N={n}(conf): setup ready {r}/{n}{tag} → 발행");
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let (stop_tx, stop_rx) = watch::channel(false);
    let sampler = tokio::spawn(sample_resources(stop_rx));
    let _ = trig_tx.send(true);
    let t0 = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = t0.elapsed().as_secs_f64();
    let _ = stop_tx.send(true);
    let res = sampler.await.unwrap_or_default();
    for h in handles {
        let _ = h.await;
    }

    use std::sync::atomic::Ordering::Relaxed;
    let tx_ok = counters.tx_ok.load(Relaxed);
    let tx_attempt = counters.tx_attempt.load(Relaxed);
    let out_pkts = counters.rx_packets.load(Relaxed);
    let lost = counters.rx_lost.load(Relaxed);
    let rx_bytes = counters.rx_bytes.load(Relaxed);
    let active = counters.active_subs.load(Relaxed);

    let in_pps = tx_ok as f64 / elapsed; // 전원 송신 합
    let out_pps = out_pkts as f64 / elapsed; // 전원 수신 합 (mesh = N×(N-1) 이상)
    let out_mbps = (rx_bytes as f64 * 8.0) / (elapsed * 1_000_000.0);
    let loss_pct = if out_pkts + lost > 0 {
        lost as f64 / (out_pkts + lost) as f64 * 100.0
    } else { 0.0 };
    let mut lats = lat.lock().await.clone();
    lats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let (lat_avg, lat_p95, lat_max) = lat_stats(&lats);

    let tx_healthy = tx_attempt == 0 || tx_ok * 100 >= tx_attempt * 99;
    let cpu_headroom = res.self_cpu_avg < (res.cores as f64 * 90.0);
    let subs_ok = n == 0 || active * 10 >= n * 9;
    let bot_healthy = tx_healthy && cpu_headroom && subs_ok;

    Row {
        n, active, in_pps, out_pps, out_mbps, loss_pct,
        lat_avg, lat_p95, lat_max,
        self_cpu: res.self_cpu_avg, self_mem_mb: res.self_mem_mb,
        sfu_cpu: res.sfu_cpu_avg, sfu_mem_mb: res.sfu_mem_mb,
        bot_healthy,
    }
}

/// avg / p95 / max (µs). 빈 입력 = 0.
fn lat_stats(sorted: &[f64]) -> (f64, f64, f64) {
    if sorted.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let avg = sorted.iter().sum::<f64>() / sorted.len() as f64;
    let p95_idx = ((sorted.len() as f64 * 0.95) as usize).min(sorted.len() - 1);
    let p95 = sorted[p95_idx];
    let max = *sorted.last().unwrap();
    (avg, p95, max)
}

fn print_row(mode: &str, r: &Row) {
    info!(
        "[{}] N={:<5} active={}/{} in_pps={:<6.0} out_pps={:<9.0} out_mbps={:<7.2} loss={:<6.3}% lat(a/p95/max µs)={:.0}/{:.0}/{:.0} bot_cpu={:.0}% sfu_cpu={:.0}% sfu_mem={:.0}MB healthy={}",
        mode, r.n, r.active, r.n, r.in_pps, r.out_pps, r.out_mbps, r.loss_pct,
        r.lat_avg, r.lat_p95, r.lat_max,
        r.self_cpu, r.sfu_cpu, r.sfu_mem_mb,
        if r.bot_healthy { "y" } else { "n" }
    );
}

fn print_table(mode: &str, rows: &[Row]) {
    println!();
    println!("╔════════════════════════════════════════════════════════════════════════════════╗");
    println!("║  CAPACITY 곡선 — mode={:<10}  (knee = 육안, 자동 판정 안 함)                  ", mode);
    println!("╚════════════════════════════════════════════════════════════════════════════════╝");
    println!(
        "  {:<6} {:>9} {:>10} {:>9} {:>7} {:>8} {:>8} {:>8} {:>9} {:>8}",
        "N", "active", "out_pps", "out_mbps", "loss%", "lat_p95", "bot_cpu", "sfu_cpu", "sfu_mem", "healthy"
    );
    println!("  {}", "-".repeat(96));
    for r in rows {
        println!(
            "  {:<6} {:>9} {:>10.0} {:>9.2} {:>7.3} {:>8.0} {:>7.0}% {:>7.0}% {:>8.0}M {:>8}",
            r.n, format!("{}/{}", r.active, r.n), r.out_pps, r.out_mbps, r.loss_pct, r.lat_p95,
            r.self_cpu, r.sfu_cpu, r.sfu_mem_mb,
            if r.bot_healthy { "y" } else { "n ←폐기" }
        );
    }
    println!();
}

/// reports/cap_{mode}_{ts}.csv 작성. 반환 = 경로.
fn write_csv(mode: &str, ts: u64, rows: &[Row]) -> std::io::Result<String> {
    std::fs::create_dir_all("reports")?;
    let path = format!("reports/cap_{mode}_{ts}.csv");
    let mut out = String::new();
    out.push_str("mode,n,active,in_pps,out_pps,out_mbps,loss_pct,lat_avg_us,lat_p95_us,lat_max_us,bot_cpu_pct,bot_mem_mb,sfu_cpu_pct,sfu_mem_mb,bot_healthy\n");
    for r in rows {
        out.push_str(&format!(
            "{},{},{},{:.1},{:.1},{:.4},{:.4},{:.0},{:.0},{:.0},{:.1},{:.1},{:.1},{:.1},{}\n",
            mode, r.n, r.active, r.in_pps, r.out_pps, r.out_mbps, r.loss_pct,
            r.lat_avg, r.lat_p95, r.lat_max,
            r.self_cpu, r.self_mem_mb, r.sfu_cpu, r.sfu_mem_mb,
            if r.bot_healthy { "y" } else { "n" }
        ));
    }
    std::fs::write(&path, out)?;
    Ok(path)
}
