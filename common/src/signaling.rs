// author: kodeholic (powered by Claude)
//! WebSocket signaling client — shared across bench / e2e tests
//!
//! Flow: connect → HELLO ← → IDENTIFY → ROOM_CREATE → ROOM_JOIN → server_config
//!       → PUBLISH_TRACKS (publisher only)

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

// ── opcodes (서버와 동일) ──

pub const OP_HELLO: u16 = 0;
pub const OP_HEARTBEAT: u16 = 1;
pub const OP_IDENTIFY: u16 = 3;
pub const OP_ROOM_CREATE: u16 = 10;
pub const OP_ROOM_JOIN: u16 = 11;
pub const OP_PUBLISH_TRACKS: u16 = 15;

// Track events (서버 opcode.rs와 동일)
pub const OP_TRACKS_UPDATE: u16 = 101;
pub const OP_TRACKS_ACK: u16 = 16;

// Floor control opcodes (서버 opcode.rs와 동일)
pub const OP_FLOOR_REQUEST: u16 = 40;
pub const OP_FLOOR_RELEASE: u16 = 41;
pub const OP_FLOOR_PING: u16 = 42;
pub const OP_FLOOR_QUEUE_POS: u16 = 43;
pub const OP_ROOM_SYNC: u16 = 50;
pub const OP_SUBSCRIBE_LAYER: u16 = 51;

// Room events (Server → Client)
pub const OP_ROOM_EVENT: u16 = 100;

// Floor events (Server → Client)
pub const OP_FLOOR_TAKEN: u16 = 141;
pub const OP_FLOOR_IDLE: u16 = 142;
pub const OP_FLOOR_REVOKE: u16 = 143;

// ── packet ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Packet {
    pub op: u16,
    pub pid: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
    #[serde(default)]
    pub d: serde_json::Value,
}

impl Packet {
    pub fn new(op: u16, pid: u64, d: serde_json::Value) -> Self {
        Self { op, pid, ok: None, d }
    }
}

// ── server_config parsed from ROOM_JOIN response ──

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub pub_ufrag: String,
    pub pub_pwd: String,
    pub sub_ufrag: String,
    pub sub_pwd: String,
    pub server_ip: String,
    pub server_port: u16,
    pub fingerprint: String,
}

// ── signaling session ──

pub struct SignalingSession {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    pid: u64,
    pub user_id: String,
    pub room_id: String,
    pub server_config: Option<ServerConfig>,
    /// 수신된 이벤트 버퍼 (request 대기 중 받은 비응답 패킷)
    event_buf: Vec<Packet>,
}

impl SignalingSession {
    /// Connect + IDENTIFY + ROOM_CREATE + ROOM_JOIN
    ///
    /// `mode`: "conference" 또는 "ptt" (ROOM_CREATE 시 전달)
    pub async fn connect(
        server: &str,
        ws_port: u16,
        user_id: &str,
        room_name: &str,
        mode: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("ws://{}:{}/ws", server, ws_port);
        info!("[sig] connecting to {}", url);

        let (ws, _) = connect_async(&url).await?;
        info!("[sig] websocket connected");

        let mut session = Self {
            ws,
            pid: 1,
            user_id: user_id.to_string(),
            room_id: String::new(),
            server_config: None,
            event_buf: Vec::new(),
        };

        // 1) Wait for HELLO
        let hello = session.recv_packet().await?;
        if hello.op != OP_HELLO {
            return Err(format!("expected HELLO, got op={}", hello.op).into());
        }
        info!("[sig] HELLO received");

        // 2) IDENTIFY
        let resp = session
            .request(OP_IDENTIFY, serde_json::json!({
                "token": "bench",
                "user_id": user_id,
            }))
            .await?;
        let assigned_id = resp.d["user_id"].as_str().unwrap_or(user_id).to_string();
        session.user_id = assigned_id.clone();
        info!("[sig] IDENTIFY ok user={}", assigned_id);

        // 3) ROOM_CREATE (with mode)
        let create_resp = session
            .request(OP_ROOM_CREATE, serde_json::json!({
                "name": room_name,
                "mode": mode,
            }))
            .await?;

        let room_id = create_resp.d["room_id"]
            .as_str()
            .ok_or("ROOM_CREATE response missing room_id")?
            .to_string();
        info!("[sig] ROOM_CREATE ok room_id={} mode={}", room_id, mode);

        // 4) ROOM_JOIN
        let resp = session
            .request(OP_ROOM_JOIN, serde_json::json!({
                "room_id": room_id,
            }))
            .await?;

        if resp.ok != Some(true) {
            return Err(format!("ROOM_JOIN failed: {:?}", resp.d).into());
        }

        let sc = &resp.d["server_config"];
        let ice = &sc["ice"];
        let dtls_section = &sc["dtls"];

        let config = ServerConfig {
            pub_ufrag: ice["publish_ufrag"].as_str().unwrap_or("").to_string(),
            pub_pwd: ice["publish_pwd"].as_str().unwrap_or("").to_string(),
            sub_ufrag: ice["subscribe_ufrag"].as_str().unwrap_or("").to_string(),
            sub_pwd: ice["subscribe_pwd"].as_str().unwrap_or("").to_string(),
            server_ip: ice["ip"].as_str().unwrap_or("127.0.0.1").to_string(),
            server_port: ice["port"].as_u64().unwrap_or(19740) as u16,
            fingerprint: dtls_section["fingerprint"]
                .as_str()
                .unwrap_or("")
                .to_string(),
        };

        info!(
            "[sig] ROOM_JOIN ok room={} pub_ufrag={} sub_ufrag={}",
            room_id, config.pub_ufrag, config.sub_ufrag,
        );

        session.room_id = room_id;
        session.server_config = Some(config);

        Ok(session)
    }

    /// Connect as subscriber to existing room (no ROOM_CREATE)
    pub async fn connect_to_room(
        server: &str,
        ws_port: u16,
        user_id: &str,
        room_id: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!("ws://{}:{}/ws", server, ws_port);
        info!("[sig:{}] connecting to {}", user_id, url);

        let (ws, _) = connect_async(&url).await?;
        let mut session = Self {
            ws,
            pid: 1,
            user_id: user_id.to_string(),
            room_id: String::new(),
            server_config: None,
            event_buf: Vec::new(),
        };

        // HELLO
        let hello = session.recv_packet().await?;
        if hello.op != OP_HELLO {
            return Err(format!("expected HELLO, got op={}", hello.op).into());
        }

        // IDENTIFY
        let resp = session
            .request(OP_IDENTIFY, serde_json::json!({
                "token": "bench",
                "user_id": user_id,
            }))
            .await?;
        let assigned_id = resp.d["user_id"].as_str().unwrap_or(user_id).to_string();
        session.user_id = assigned_id.clone();

        // ROOM_JOIN (existing room)
        let resp = session
            .request(OP_ROOM_JOIN, serde_json::json!({
                "room_id": room_id,
            }))
            .await?;

        if resp.ok != Some(true) {
            return Err(format!("ROOM_JOIN failed: {:?}", resp.d).into());
        }

        let sc = &resp.d["server_config"];
        let ice = &sc["ice"];
        let dtls_section = &sc["dtls"];

        let config = ServerConfig {
            pub_ufrag: ice["publish_ufrag"].as_str().unwrap_or("").to_string(),
            pub_pwd: ice["publish_pwd"].as_str().unwrap_or("").to_string(),
            sub_ufrag: ice["subscribe_ufrag"].as_str().unwrap_or("").to_string(),
            sub_pwd: ice["subscribe_pwd"].as_str().unwrap_or("").to_string(),
            server_ip: ice["ip"].as_str().unwrap_or("127.0.0.1").to_string(),
            server_port: ice["port"].as_u64().unwrap_or(19740) as u16,
            fingerprint: dtls_section["fingerprint"].as_str().unwrap_or("").to_string(),
        };

        info!(
            "[sig:{}] ROOM_JOIN ok sub_ufrag={}",
            assigned_id, config.sub_ufrag,
        );

        session.room_id = room_id.to_string();
        session.server_config = Some(config);
        Ok(session)
    }

    /// Send PUBLISH_TRACKS to register publisher SSRCs
    pub async fn publish_tracks(
        &mut self,
        tracks: Vec<(String, u32)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let items: Vec<serde_json::Value> = tracks
            .iter()
            .map(|(kind, ssrc)| serde_json::json!({ "kind": kind, "ssrc": ssrc }))
            .collect();

        let resp = self
            .request(OP_PUBLISH_TRACKS, serde_json::json!({ "tracks": items }))
            .await?;

        if resp.ok != Some(true) {
            return Err(format!("PUBLISH_TRACKS failed: {:?}", resp.d).into());
        }
        info!("[sig] PUBLISH_TRACKS ok");
        Ok(())
    }

    /// Send heartbeat (keep WS alive)
    pub async fn heartbeat(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let _ = self.request(OP_HEARTBEAT, serde_json::json!({})).await?;
        Ok(())
    }

    // ── Floor Control (WS) ──

    /// WS Floor Request with priority (v2)
    /// Returns the raw response packet (ok=true with granted/queued, or ok=false with deny)
    pub async fn floor_request(
        &mut self,
        room_id: &str,
        priority: u8,
    ) -> Result<Packet, Box<dyn std::error::Error + Send + Sync>> {
        self.request(OP_FLOOR_REQUEST, serde_json::json!({
            "room_id": room_id,
            "priority": priority,
        })).await
    }

    /// WS Floor Release
    pub async fn floor_release(
        &mut self,
        room_id: &str,
    ) -> Result<Packet, Box<dyn std::error::Error + Send + Sync>> {
        self.request(OP_FLOOR_RELEASE, serde_json::json!({
            "room_id": room_id,
        })).await
    }

    /// WS Floor Queue Position query
    pub async fn floor_queue_pos(
        &mut self,
        room_id: &str,
    ) -> Result<Packet, Box<dyn std::error::Error + Send + Sync>> {
        self.request(OP_FLOOR_QUEUE_POS, serde_json::json!({
            "room_id": room_id,
        })).await
    }

    /// SUBSCRIBE_LAYER (op=51) — simulcast 레이어 선택
    pub async fn subscribe_layer(
        &mut self,
        targets: Vec<serde_json::Value>,
    ) -> Result<Packet, Box<dyn std::error::Error + Send + Sync>> {
        self.request(OP_SUBSCRIBE_LAYER, serde_json::json!({
            "targets": targets,
        })).await
    }

    /// 이벤트 버퍼에서 특정 opcode를 가진 패킷을 꺼냄 (없으면 None)
    pub fn drain_event(&mut self, op: u16) -> Option<Packet> {
        if let Some(idx) = self.event_buf.iter().position(|p| p.op == op) {
            Some(self.event_buf.remove(idx))
        } else {
            None
        }
    }

    /// 이벤트 버퍼에서 특정 opcode 패킷 전부 꺼냄
    pub fn drain_all_events(&mut self, op: u16) -> Vec<Packet> {
        let mut matched = Vec::new();
        self.event_buf.retain(|p| {
            if p.op == op {
                matched.push(p.clone());
                false
            } else {
                true
            }
        });
        matched
    }

    /// 이벤트 버퍼에 쌓인 WS 메시지 수신 (non-blocking poll)
    /// heartbeat/hold 루프에서 호출하여 event_buf를 채움
    pub async fn poll_events(&mut self) {
        loop {
            match tokio::time::timeout(
                std::time::Duration::from_millis(1),
                self.recv_packet(),
            ).await {
                Ok(Ok(pkt)) => self.event_buf.push(pkt),
                _ => break,
            }
        }
    }

    /// 지정 timeout 내에 특정 opcode 이벤트 수신 대기
    pub async fn wait_event(
        &mut self,
        op: u16,
        timeout: std::time::Duration,
    ) -> Option<Packet> {
        // 버퍼에 이미 있으면 즉시 반환
        if let Some(pkt) = self.drain_event(op) {
            return Some(pkt);
        }

        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }

            match tokio::time::timeout(remaining, self.recv_packet()).await {
                Ok(Ok(pkt)) => {
                    if pkt.op == op {
                        return Some(pkt);
                    }
                    // 다른 이벤트는 버퍼에 저장
                    self.event_buf.push(pkt);
                }
                Ok(Err(e)) => {
                    warn!("[sig] recv error while waiting for op={}: {}", op, e);
                    return None;
                }
                Err(_) => return None, // timeout
            }
        }
    }

    /// Close WS connection
    pub async fn close(&mut self) {
        let _ = self.ws.close(None).await;
    }

    // ── internal ──

    fn next_pid(&mut self) -> u64 {
        let p = self.pid;
        self.pid += 1;
        p
    }

    async fn send_packet(
        &mut self,
        packet: &Packet,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let json = serde_json::to_string(packet)?;
        debug!("[sig] → {}", json);
        self.ws.send(Message::Text(json.into())).await?;
        Ok(())
    }

    async fn recv_packet(
        &mut self,
    ) -> Result<Packet, Box<dyn std::error::Error + Send + Sync>> {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let text_str: &str = &text;
                    debug!("[sig] ← {}", text_str);
                    let pkt: Packet = serde_json::from_str(text_str)?;
                    return Ok(pkt);
                }
                Some(Ok(Message::Close(_))) | None => {
                    return Err("ws closed".into());
                }
                Some(Err(e)) => {
                    return Err(format!("ws error: {e}").into());
                }
                _ => continue,
            }
        }
    }

    /// Send request, wait for response with matching op (events buffered)
    pub async fn request(
        &mut self,
        op: u16,
        d: serde_json::Value,
    ) -> Result<Packet, Box<dyn std::error::Error + Send + Sync>> {
        let pid = self.next_pid();
        let pkt = Packet::new(op, pid, d);
        self.send_packet(&pkt).await?;

        loop {
            let resp = self.recv_packet().await?;
            if resp.op == op && resp.ok.is_some() {
                return Ok(resp);
            }
            // Server events → buffer
            self.event_buf.push(resp);
        }
    }
}
