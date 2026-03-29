// author: kodeholic (powered by Claude)
//! oxlab — OxLabs 통합 CLI
//!
//! Phase 0: `oxlab run` — 봇 N개 spawn + SFU 접속 + 방 입장
//! Phase 1: `oxlab run --media` — 미디어 셋업 + Fake RTP 전송
//!
//! 설정 우선순위: CLI 인자 > 환경변수(OXLAB_*) > .env 파일 > 기본값
//!
//! Usage:
//!   oxlab run                                    # .env 기본값 사용
//!   oxlab run --media --hold 10                  # .env + 일부 오버라이드
//!   oxlab run --server 10.0.0.1 --port 9222      # 전체 명시

use clap::{Parser, Subcommand};
use oxlab_bot::{Bot, BotConfig, BotStatus};
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
        #[arg(long, env = "OXLAB_SERVER", default_value = "127.0.0.1")]
        server: String,

        /// WS 포트
        #[arg(long, env = "OXLAB_WS_PORT", default_value_t = 9222)]
        port: u16,

        /// 방 이름
        #[arg(long, env = "OXLAB_ROOM", default_value = "oxlab-test")]
        room: String,

        /// 방 모드 (conference | ptt)
        #[arg(long, env = "OXLAB_MODE", default_value = "conference")]
        mode: String,

        /// 봇 수
        #[arg(long, env = "OXLAB_BOTS", default_value_t = 3)]
        bots: usize,

        /// 네트워크 프로파일 (builtin: pristine/office_wifi/field_lte/field_lte_poor/basement)
        #[arg(long, env = "OXLAB_PROFILE")]
        profile: Option<String>,

        /// 접속 후 유지 시간 (초, 0 = 즉시 종료)
        #[arg(long, env = "OXLAB_HOLD", default_value_t = 10)]
        hold: u64,

        /// 미디어 활성화 (STUN+DTLS+SRTP + Fake RTP 전송)
        #[arg(long, default_value_t = false)]
        media: bool,
    },
}

#[tokio::main]
async fn main() {
    // .env 파일 로드 (없어도 에러 안 남)
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            server, port, room, mode,
            bots, profile, hold, media,
        } => {
            cmd_run(server, port, room, mode, bots, profile, hold, media).await;
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
    media_enabled: bool,
) {
    let phase = if media_enabled { "Phase 1 (media)" } else { "Phase 0 (signaling)" };
    info!("=== OxLabs {} ===", phase);
    info!(
        "server={}:{} room={} mode={} bots={} profile={} hold={}s media={}",
        server, port, room, mode, bot_count,
        profile_name.as_deref().unwrap_or("pristine"),
        hold_secs, media_enabled
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

    // ── 봇 생성 + 시그널링 접속 ──
    let mut bots = Vec::new();
    let mut room_id: Option<String> = None;

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
            let rid = room_id.as_deref().expect("room_id should be set by bot_1");
            match bot.join_existing_room(rid).await {
                Ok(()) => {}
                Err(e) => {
                    error!("[{}] failed to join: {}", bot_id, e);
                    continue;
                }
            }
        }

        bots.push(bot);
    }

    let joined = bots.iter().filter(|b| b.status == BotStatus::Joined).count();
    info!("=== {} / {} bots joined ===", joined, bot_count);

    // ── 미디어 셋업 + Publishing ──
    if media_enabled {
        info!("── media setup ──");
        for bot in &mut bots {
            if bot.status != BotStatus::Joined { continue; }
            let id = bot.id().to_string();

            if let Err(e) = bot.setup_media().await {
                error!("[{}] media setup failed: {}", id, e);
                continue;
            }
            if let Err(e) = bot.publish_intent().await {
                error!("[{}] publish_intent failed: {}", id, e);
                continue;
            }
            if let Err(e) = bot.start_publishing() {
                error!("[{}] start_publishing failed: {}", id, e);
                continue;
            }
        }

        let publishing = bots.iter().filter(|b| b.status == BotStatus::Publishing).count();
        info!("=== {} / {} bots publishing ===", publishing, bot_count);

        // TRACKS_UPDATE 이벤트 좌식 처리 → TRACKS_ACK 전송 (SubscriberGate 5초 지연 해소)
        // RTP stream discovery는 비동기이므로 약간의 대기 필요
        tokio::time::sleep(Duration::from_secs(2)).await;
        for bot in &mut bots {
            if bot.status == BotStatus::Publishing {
                if let Err(e) = bot.process_events().await {
                    error!("[{}] process_events failed: {}", bot.id(), e);
                }
            }
        }
    }

    // ── PTT 라운드로빈 사이클 (WS 전용) ──
    if media_enabled && mode == "ptt" {
        info!("── PTT round-robin cycle (WS) ──");
        let talk_secs = 3u64;
        let gap_secs = 1u64;

        for i in 0..bots.len() {
            if bots[i].status != BotStatus::Publishing { continue; }

            let bot_id = bots[i].id().to_string();
            info!("[PTT] {} floor_request (WS)", bot_id);

            let granted = match bots[i].floor_request_ws(0).await {
                Ok(g) => g,
                Err(e) => {
                    error!("[{}] floor_request_ws failed: {}", bot_id, e);
                    false
                }
            };

            if granted {
                info!("[PTT] {} talking for {}s...", bot_id, talk_secs);
                tokio::time::sleep(Duration::from_secs(talk_secs)).await;

                info!("[PTT] {} floor_release (WS)", bot_id);
                if let Err(e) = bots[i].floor_release_ws().await {
                    error!("[{}] floor_release_ws failed: {}", bot_id, e);
                }
            }

            // 화자 간 간격
            if i + 1 < bots.len() {
                tokio::time::sleep(Duration::from_secs(gap_secs)).await;
            }
        }
        info!("── PTT cycle complete ──");
    }

    // ── Hold (heartbeat + recv metrics) ──
    if hold_secs > 0 {
        info!("holding for {}s (heartbeat every 5s)...", hold_secs);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(hold_secs);

        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            if tokio::time::Instant::now() >= deadline {
                break;
            }

            for bot in &mut bots {
                let active = bot.status == BotStatus::Joined
                    || bot.status == BotStatus::MediaReady
                    || bot.status == BotStatus::Publishing;
                if active {
                    if let Err(e) = bot.heartbeat().await {
                        error!("[{}] heartbeat failed: {}", bot.id(), e);
                    }
                }
                if bot.status == BotStatus::Publishing {
                    if let Err(e) = bot.process_events().await {
                        error!("[{}] process_events failed: {}", bot.id(), e);
                    }
                    bot.log_recv_metrics();
                }
            }
        }
    }

    // ── 최종 수신 메트릭 ──
    if media_enabled {
        info!("── final recv metrics ──");
        for bot in &bots {
            bot.log_recv_metrics();
        }
    }

    // ── 전체 봇 종료 ──
    for bot in &mut bots {
        bot.disconnect().await;
    }

    info!("=== OxLabs {} complete ===", phase);
}
