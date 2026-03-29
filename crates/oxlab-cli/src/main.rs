// author: kodeholic (powered by Claude)
//! oxlab — OxLabs 통합 CLI
//!
//! Phase 0: `oxlab run` — 봇 N개 spawn + SFU 접속 + 방 입장
//!
//! Usage:
//!   oxlab run --server 127.0.0.1 --port 9222 --room test --bots 3
//!   oxlab run --server 127.0.0.1 --room test --bots 5 --mode ptt --profile field_lte

use clap::{Parser, Subcommand};
use oxlab_bot::{Bot, BotConfig};
use oxlab_net::{NetFilter, NetworkProfile};
use std::time::Duration;
use tracing::{error, info};

#[derive(Parser)]
#[command(name = "oxlab", about = "OxLabs — SFU quality loop test runner")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 봇을 spawn하여 SFU에 접속
    Run {
        /// SFU 서버 주소
        #[arg(long, default_value = "127.0.0.1")]
        server: String,

        /// WS 포트
        #[arg(long, default_value_t = 9222)]
        port: u16,

        /// 방 이름
        #[arg(long, default_value = "oxlab-test")]
        room: String,

        /// 방 모드 (conference | ptt)
        #[arg(long, default_value = "conference")]
        mode: String,

        /// 봇 수
        #[arg(long, default_value_t = 3)]
        bots: usize,

        /// 네트워크 프로파일 (전 봇 공통, builtin: pristine/office_wifi/field_lte/field_lte_poor/basement)
        #[arg(long)]
        profile: Option<String>,

        /// 접속 후 유지 시간 (초, 0 = 즉시 종료)
        #[arg(long, default_value_t = 10)]
        hold: u64,
    },
}

#[tokio::main]
async fn main() {
    // tracing 초기화
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            server,
            port,
            room,
            mode,
            bots,
            profile,
            hold,
        } => {
            cmd_run(server, port, room, mode, bots, profile, hold).await;
        }
    }
}

async fn cmd_run(
    server: String,
    port: u16,
    room: String,
    mode: String,
    bot_count: usize,
    profile_name: Option<String>,
    hold_secs: u64,
) {
    info!("=== OxLabs Phase 0 ===");
    info!(
        "server={}:{} room={} mode={} bots={} profile={} hold={}s",
        server,
        port,
        room,
        mode,
        bot_count,
        profile_name.as_deref().unwrap_or("pristine"),
        hold_secs
    );

    // 네트워크 프로파일 로드
    let net_profile = match &profile_name {
        Some(name) => NetworkProfile::builtin(name).unwrap_or_else(|| {
            error!("unknown builtin profile '{}', falling back to pristine", name);
            NetworkProfile::pristine()
        }),
        None => NetworkProfile::pristine(),
    };

    info!(
        "network profile: {} (loss={:.1}% delay={}ms jitter={}ms bw={}kbps)",
        net_profile.meta.name,
        net_profile.conditions.loss_percent,
        net_profile.conditions.delay_ms,
        net_profile.conditions.jitter_ms,
        net_profile.conditions.bandwidth_kbps
    );

    // 봇 생성 + 접속
    let mut handles = Vec::new();
    let mut room_id: Option<String> = None;

    // 첫 번째 봇이 방을 생성하고, 나머지는 기존 방에 참가
    for i in 0..bot_count {
        let bot_id = format!("bot_{}", i + 1);
        let config = BotConfig {
            id: bot_id.clone(),
            server: server.clone(),
            ws_port: port,
            room_name: room.clone(),
            mode: mode.clone(),
            profile: profile_name.clone(),
        };

        let filter = NetFilter::new(net_profile.conditions.clone());
        let mut bot = Bot::new(config, Some(filter));

        if i == 0 {
            // 첫 봇: 방 생성 + 입장
            match bot.connect_and_join().await {
                Ok(()) => {
                    room_id = bot.room_id.clone();
                    info!("[{}] room created: {:?}", bot_id, room_id);
                }
                Err(e) => {
                    error!("[{}] failed to connect: {}", bot_id, e);
                    return;
                }
            }
        } else {
            // 나머지: 기존 방에 참가
            let rid = room_id.as_deref().expect("room_id should be set by bot_1");
            match bot.join_existing_room(rid).await {
                Ok(()) => {}
                Err(e) => {
                    error!("[{}] failed to join: {}", bot_id, e);
                    continue;
                }
            }
        }

        handles.push(bot);
    }

    let joined = handles.iter().filter(|b| b.status == oxlab_bot::BotStatus::Joined).count();
    info!("=== {} / {} bots joined ===", joined, bot_count);

    // hold 시간 동안 유지 (heartbeat)
    if hold_secs > 0 {
        info!("holding for {}s (heartbeat every 5s)...", hold_secs);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(hold_secs);

        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            if tokio::time::Instant::now() >= deadline {
                break;
            }

            for bot in &mut handles {
                if bot.status == oxlab_bot::BotStatus::Joined {
                    if let Err(e) = bot.heartbeat().await {
                        error!("[{}] heartbeat failed: {}", bot.id(), e);
                    }
                }
            }
        }
    }

    // 전체 봇 종료
    for bot in &mut handles {
        bot.disconnect().await;
    }

    info!("=== OxLabs Phase 0 complete ===");
}
