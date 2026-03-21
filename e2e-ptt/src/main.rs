// author: kodeholic (powered by Claude)
//! e2e-ptt — PTT Floor Control E2E test (v2: priority + queuing + preemption)
//!
//! 서버(oxlens-sfu-server)를 실제로 띄운 상태에서 실행.
//!
//! Part 1 — MBCP scenarios (full media pipeline):
//!   1. basic_grant_release:     A FREQ → FTKN → FREL → FIDL
//!   2. queued_when_busy:        A 발화 + B FREQ → B Queued (v2 큐잉)
//!   3. floor_switch:            A FREL 후 B FREQ → B FTKN
//!   4. rtp_gating:              비발화자 RTP 차단 확인
//!
//! Part 2 — WS Floor v2 scenarios (signaling only):
//!   5. ws_priority_queuing:     A(pri=5) + B(pri=2) → B Queued
//!   6. ws_preemption:           A(pri=2) + B(pri=5) → A revoked, B granted
//!   7. ws_queue_pop_on_release: A + B큐 → A release → B 자동 granted
//!   8. ws_queue_position:       A + B,C큐 → 우선순위 정렬 + 큐 위치 조회

mod scenario;

use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "e2e-ptt", about = "PTT Floor Control E2E test")]
pub struct Args {
    /// SFU server host
    #[arg(long, default_value = "127.0.0.1")]
    pub server: String,

    /// WebSocket port
    #[arg(long, default_value_t = 1974)]
    pub ws_port: u16,

    /// UDP media port
    #[arg(long, default_value_t = 19740)]
    pub udp_port: u16,

    /// Room name
    #[arg(long, default_value = "e2e-ptt-test")]
    pub room: String,

    /// Run specific test (all if omitted)
    #[arg(long)]
    pub test: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "e2e_ptt=info,oxlens_lab_common=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    info!("=== E2E-PTT v{} ===", env!("CARGO_PKG_VERSION"));
    info!("server: {}:{} (ws:{})", args.server, args.udp_port, args.ws_port);

    let tests: Vec<(&str, fn(&Args) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + '_>>)> = vec![
        // Part 1: MBCP scenarios
        ("basic_grant_release",    |a| Box::pin(scenario::test_basic_grant_release(a))),
        ("queued_when_busy",       |a| Box::pin(scenario::test_queued_when_busy(a))),
        ("floor_switch",           |a| Box::pin(scenario::test_floor_switch(a))),
        ("rtp_gating",             |a| Box::pin(scenario::test_rtp_gating(a))),
        // Part 2: WS Floor v2 scenarios
        ("ws_priority_queuing",    |a| Box::pin(scenario::test_ws_priority_queuing(a))),
        ("ws_preemption",          |a| Box::pin(scenario::test_ws_preemption(a))),
        ("ws_queue_pop_on_release",|a| Box::pin(scenario::test_ws_queue_pop_on_release(a))),
        ("ws_queue_position",      |a| Box::pin(scenario::test_ws_queue_position(a))),
    ];

    let mut passed = 0;
    let mut failed = 0;
    let mut skipped = 0;

    for (name, test_fn) in &tests {
        if let Some(ref filter) = args.test {
            if !name.contains(filter.as_str()) {
                skipped += 1;
                continue;
            }
        }

        info!("──── {} ────", name);
        match test_fn(&args).await {
            Ok(()) => {
                info!("  ✅ {} PASSED", name);
                passed += 1;
            }
            Err(e) => {
                tracing::error!("  ❌ {} FAILED: {}", name, e);
                failed += 1;
            }
        }
    }

    println!();
    println!("═══════════════════════════════════");
    println!("  E2E-PTT Results: {} passed, {} failed, {} skipped",
        passed, failed, skipped);
    println!("═══════════════════════════════════");

    if failed > 0 {
        std::process::exit(1);
    }
}
