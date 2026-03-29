// author: kodeholic (powered by Claude)
//! Bot — SFU에 접속하는 가짜 참가자
//!
//! Phase 0: 시그널링(WS) 접속 + 방 입장까지
//! Phase 1: RTP 송수신 + 메트릭 수집

use oxlab_net::NetFilter;
use oxlens_lab_common::signaling::SignalingSession;
use serde::{Deserialize, Serialize};
use tracing::info;

// ── Bot 상태 ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BotStatus {
    Created,
    Connected,  // WS 연결됨
    Joined,     // 방 입장 완료
    Publishing, // RTP 전송 중 (Phase 1)
    Failed,
    Stopped,
}

// ── Bot 설정 ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotConfig {
    /// 봇 식별자 (e.g., "bot_1")
    pub id: String,

    /// SFU 서버 주소 (e.g., "127.0.0.1")
    pub server: String,

    /// WS 포트
    #[serde(default = "default_ws_port")]
    pub ws_port: u16,

    /// 입장할 방 이름
    pub room_name: String,

    /// 방 모드 ("conference" | "ptt")
    #[serde(default = "default_mode")]
    pub mode: String,

    /// 네트워크 프로파일 이름 (None = pristine)
    pub profile: Option<String>,
}

fn default_ws_port() -> u16 {
    9222
}

fn default_mode() -> String {
    "conference".to_string()
}

// ── Bot 본체 ──

pub struct Bot {
    pub config: BotConfig,
    pub status: BotStatus,
    pub user_id: Option<String>,
    pub room_id: Option<String>,
    session: Option<SignalingSession>,
    /// Phase 1에서 RTP 송수신 시 사용
    #[allow(dead_code)]
    net_filter: Option<NetFilter>,
}

impl Bot {
    pub fn new(config: BotConfig, net_filter: Option<NetFilter>) -> Self {
        Self {
            config,
            status: BotStatus::Created,
            user_id: None,
            room_id: None,
            session: None,
            net_filter,
        }
    }

    /// WS 연결 + IDENTIFY + ROOM_CREATE + ROOM_JOIN
    pub async fn connect_and_join(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("[bot:{}] connecting to {}:{}", self.config.id, self.config.server, self.config.ws_port);

        let session = SignalingSession::connect(
            &self.config.server,
            self.config.ws_port,
            &self.config.id,
            &self.config.room_name,
            &self.config.mode,
        )
        .await?;

        let user_id = session.user_id.clone();
        let room_id = session.room_id.clone();

        info!(
            "[bot:{}] joined room={} user_id={}",
            self.config.id, room_id, user_id
        );

        self.user_id = Some(user_id);
        self.room_id = Some(room_id);
        self.session = Some(session);
        self.status = BotStatus::Joined;

        Ok(())
    }

    /// 기존 방에 참가 (ROOM_CREATE 생략)
    pub async fn join_existing_room(
        &mut self,
        room_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "[bot:{}] joining existing room={} at {}:{}",
            self.config.id, room_id, self.config.server, self.config.ws_port
        );

        let session = SignalingSession::connect_to_room(
            &self.config.server,
            self.config.ws_port,
            &self.config.id,
            room_id,
        )
        .await?;

        let user_id = session.user_id.clone();
        let room_id = session.room_id.clone();

        info!("[bot:{}] joined user_id={}", self.config.id, user_id);

        self.user_id = Some(user_id);
        self.room_id = Some(room_id);
        self.session = Some(session);
        self.status = BotStatus::Joined;

        Ok(())
    }

    /// Heartbeat 전송
    pub async fn heartbeat(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(ref mut session) = self.session {
            session.heartbeat().await?;
        }
        Ok(())
    }

    /// WS 연결 종료
    pub async fn disconnect(&mut self) {
        if let Some(ref mut session) = self.session {
            session.close().await;
        }
        self.status = BotStatus::Stopped;
        info!("[bot:{}] disconnected", self.config.id);
    }

    /// 봇 ID 참조
    pub fn id(&self) -> &str {
        &self.config.id
    }
}
