// author: kodeholic (powered by Claude)
//! e2e-ptt — PTT Floor Control E2E test
//!
//! 서버(oxlens-sfu-server)를 실제로 띄운 상태에서 실행.
//! 2~3명의 가상 참가자가 WS 시그널링 + STUN + DTLS + SRTP 풀 파이프라인을 연결하고,
//! RTCP APP(MBCP) 패킷을 통한 Floor Control 시나리오를 검증한다.
//!
//! 테스트 시나리오:
//!   1. basic_grant_release:  A FREQ → FTKN 수신 → RTP 전송 → B 수신 → FREL → FIDL
//!   2. deny_when_busy:       A 발화 중 B FREQ → B가 FRVK(denied) 수신
//!   3. floor_switch:         A FREL 후 B FREQ → B FTKN
//!   4. rtp_gating:           비발화자의 RTP가 subscriber에 도달하지 않음 확인

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
        ("basic_grant_release", |a| Box::pin(scenario::test_basic_grant_release(a))),
        ("deny_when_busy",      |a| Box::pin(scenario::test_deny_when_busy(a))),
        ("floor_switch",        |a| Box::pin(scenario::test_floor_switch(a))),
        ("rtp_gating",          |a| Box::pin(scenario::test_rtp_gating(a))),
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
