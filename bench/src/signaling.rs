// author: kodeholic (powered by Claude)
//! WebSocket signaling client for sfu-bench
//!
//! Flow: connect → HELLO ← → IDENTIFY → ROOM_JOIN → server_config
//!       → PUBLISH_TRACKS (publisher only)

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info};

// ── opcodes (서버와 동일) ──

const OP_HELLO: u16 = 0;
const OP_HEARTBEAT: u16 = 1;
const OP_IDENTIFY: u16 = 3;
const OP_ROOM_CREATE: u16 = 10;
const OP_ROOM_JOIN: u16 = 11;
const OP_PUBLISH_TRACKS: u16 = 15;

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
#[allow(dead_code)] // sub_pwd, fingerprint used in Phase B-3 (subscriber)
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
}

impl SignalingSession {
    /// Connect to SFU, identify, create/join room, return session.
    pub async fn connect(
        server: &str,
        ws_port: u16,
        user_id: &str,
        room_name: &str,
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

        // 3) ROOM_CREATE → server returns room_id (UUID)
        let create_resp = session
            .request(OP_ROOM_CREATE, serde_json::json!({
                "name": room_name,
            }))
            .await?;

        let room_id = create_resp.d["room_id"]
            .as_str()
            .ok_or("ROOM_CREATE response missing room_id")?
            .to_string();
        info!("[sig] ROOM_CREATE ok room_id={}", room_id);

        // 4) ROOM_JOIN (use server-assigned room_id, not name)
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

    /// Send PUBLISH_TRACKS to register fake publisher SSRC
    pub async fn publish_tracks(
        &mut self,
        tracks: Vec<(String, u32)>, // (kind, ssrc)
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let items: Vec<serde_json::Value> = tracks
            .iter()
            .map(|(kind, ssrc)| {
                serde_json::json!({ "kind": kind, "ssrc": ssrc })
            })
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

    /// Send heartbeat (keep WS alive during long benchmarks)
    pub async fn heartbeat(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let _ = self
            .request(OP_HEARTBEAT, serde_json::json!({}))
            .await?;
        Ok(())
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

        // ROOM_JOIN (existing room, no ROOM_CREATE)
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

    /// Send request, wait for response with matching op.
    async fn request(
        &mut self,
        op: u16,
        d: serde_json::Value,
    ) -> Result<Packet, Box<dyn std::error::Error + Send + Sync>> {
        let pid = self.next_pid();
        let pkt = Packet::new(op, pid, d);
        self.send_packet(&pkt).await?;

        // Wait for response (skip events)
        loop {
            let resp = self.recv_packet().await?;
            if resp.op == op && resp.ok.is_some() {
                return Ok(resp);
            }
            // Server events — ignore during setup
            debug!("[sig] skipping event op={}", resp.op);
        }
    }
}
