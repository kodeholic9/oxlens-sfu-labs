// author: kodeholic (powered by Claude)
//! sfu-bench — SFU benchmark client for light-livechat
//!
//! Modes:
//!   fanout     — 1 publisher → N subscribers (fan-out throughput)
//!   conference — N participants, all publish + all subscribe (meeting room)

mod signaling;
mod stun;
mod media;
mod conference;
mod report;

use clap::Parser;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "sfu-bench", about = "SFU benchmark client for light-livechat")]
pub struct Args {
    /// SFU server host
    #[arg(long, default_value = "127.0.0.1")]
    pub server: String,

    /// WebSocket port
    #[arg(long, default_value_t = 19741)]
    pub ws_port: u16,

    /// UDP media port
    #[arg(long, default_value_t = 19740)]
    pub udp_port: u16,

    /// Benchmark mode: fanout or conference
    #[arg(long, default_value = "fanout")]
    pub mode: String,

    /// Number of publisher clients (fanout mode)
    #[arg(long, default_value_t = 1)]
    pub publishers: u32,

    /// Number of subscriber clients (fanout mode)
    #[arg(long, default_value_t = 0)]
    pub subscribers: u32,

    /// Number of participants (conference mode, all pub+sub)
    #[arg(long, default_value_t = 2)]
    pub participants: u32,

    /// Test duration in seconds
    #[arg(long, default_value_t = 30)]
    pub duration: u64,

    /// Fake RTP send rate (frames per second)
    #[arg(long, default_value_t = 30)]
    pub fps: u32,

    /// RTP payload size in bytes
    #[arg(long, default_value_t = 1200)]
    pub pkt_size: usize,

    /// Room name for benchmark
    #[arg(long, default_value = "bench")]
    pub room: String,

    /// Label for this benchmark run (used in report)
    #[arg(long, default_value = "baseline")]
    pub label: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sfu_bench=info".parse().unwrap()),
        )
        .init();

    let args = Args::parse();

    info!("=== SFU-BENCH v{} ===", env!("CARGO_PKG_VERSION"));
    info!("server: {}:{} (ws:{})", args.server, args.udp_port, args.ws_port);

    match args.mode.as_str() {
        "conference" | "conf" => {
            info!("mode: conference, {} participants, {}s, {}fps, {}B",
                args.participants, args.duration, args.fps, args.pkt_size);

            match conference::run_conference(&args).await {
                Ok(result) => {
                    report::print_conference_report(&args, &result);
                }
                Err(e) => {
                    tracing::error!("benchmark failed: {}", e);
                    std::process::exit(1);
                }
            }
        }
        _ => {
            info!("mode: fanout, {} pub, {} sub, {}s, {}fps, {}B",
                args.publishers, args.subscribers, args.duration, args.fps, args.pkt_size);

            match media::run_benchmark(&args).await {
                Ok(result) => {
                    report::print_report(&args, &result);
                }
                Err(e) => {
                    tracing::error!("benchmark failed: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }
}
