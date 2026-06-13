// author: kodeholic (powered by Claude)
//! WebSocket signaling client — wire v3 (8B 헤더 + 16진 opcode)
//!
//! Flow: connect → HELLO ← → IDENTIFY → IDENTIFY_RESULT ← → ROOM_CREATE → ROOM_JOIN
//!       → server_config (sfud UDP 직결 좌표)
//!
//! wire v3 (2026-05-16, 단일 출처 `oxsig`):
//! - WS Binary frame 단일. 모든 메시지 = `[8B WireHeader][body]`.
//! - 헤더 = `[ver=0x01, flags(ack_state), op(2 BE), pid(4 BE)]`.
//! - body: JSON (FLOOR_MBCP 0x2400 만 MBCP TLV binary).
//! - Handshake(0x00xx) = pid=0, ACK 없음. IDENTIFY 응답은 IDENTIFY_RESULT(다른 op, MSG).
//! - Request(0x1xxx) = ACK = 동일 op + 동일 pid 응답 (ack_state AckOk/AckFail).
//! - Event(0x2xxx, S→C) = **클라 ACK 필수** — 수신 루프가 ACK_OK 자동 회신
//!   (미회신 시 hub OutboundQueue ACK timeout → 연결 끊김). FLOOR_MBCP 는 예외(no-ack 우회).
//!
//! opcode/헤더 = oxsig 단일 출처 참조 (자체 복제 금지, OXLABS_DESIGN §3 의도).

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

use oxsig::header::{split_frame, WireAckState, WireHeader};
use oxsig::opcode;

// ── opcode re-export (봇/시나리오 호환 별칭) ──
//
// v3 단일 출처는 oxsig::opcode. 아래는 기존 호출처(OP_*) 호환 별칭.
// v2→v3 개명: TRACKS_ACK → TRACKS_READY. 폐기: FLOOR_REQUEST/RELEASE/TAKEN/IDLE/REVOKE
// (전부 FLOOR_MBCP self-describing 으로 통합 — mbcp.rs 파싱).
pub use oxsig::opcode::{
    HELLO as OP_HELLO,
    HEARTBEAT as OP_HEARTBEAT,
    IDENTIFY as OP_IDENTIFY,
    IDENTIFY_RESULT as OP_IDENTIFY_RESULT,
    ROOM_CREATE as OP_ROOM_CREATE,
    ROOM_JOIN as OP_ROOM_JOIN,
    ROOM_LEAVE as OP_ROOM_LEAVE,
    ROOM_EVENT as OP_ROOM_EVENT,
    PUBLISH_TRACKS as OP_PUBLISH_TRACKS,
    TRACKS_READY as OP_TRACKS_READY,
    TRACKS_UPDATE as OP_TRACKS_UPDATE,
    SUBSCRIBE_LAYER as OP_SUBSCRIBE_LAYER,
    FLOOR_MBCP as OP_FLOOR_MBCP,
};

// ── packet (application-level view, v2 호환 인터페이스) ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Packet {
    pub op: u16,
    /// v3: per-connection monotonic u32 (handshake = 0).
    pub pid: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ok: Option<bool>,
    #[serde(default)]
    pub d: serde_json::Value,
}

impl Packet {
    pub fn new(op: u16, pid: u32, d: serde_json::Value) -> Self {
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

impl ServerConfig {
    /// ROOM_JOIN ACK body 의 `server_config` 객체에서 파싱.
    fn parse(d: &serde_json::Value) -> Self {
        let sc = &d["server_config"];
        let ice = &sc["ice"];
        let dtls = &sc["dtls"];
        Self {
            pub_ufrag: ice["publish_ufrag"].as_str().unwrap_or("").to_string(),
            pub_pwd: ice["publish_pwd"].as_str().unwrap_or("").to_string(),
            sub_ufrag: ice["subscribe_ufrag"].as_str().unwrap_or("").to_string(),
            sub_pwd: ice["subscribe_pwd"].as_str().unwrap_or("").to_string(),
            server_ip: ice["ip"].as_str().unwrap_or("127.0.0.1").to_string(),
            server_port: ice["port"].as_u64().unwrap_or(19740) as u16,
            fingerprint: dtls["fingerprint"].as_str().unwrap_or("").to_string(),
        }
    }
}

// ── 수신 프레임 (헤더 + raw body) ──

struct RecvFrame {
    header: WireHeader,
    body: Vec<u8>,
}

type DynErr = Box<dyn std::error::Error + Send + Sync>;

// ── signaling session ──

pub struct SignalingSession {
    ws: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    pid: u32,
    pub user_id: String,
    pub room_id: String,
    pub server_config: Option<ServerConfig>,
    /// 수신된 JSON 이벤트 버퍼 (request 대기 중 받은 비응답 패킷)
    event_buf: Vec<Packet>,
    /// 수신된 FLOOR_MBCP raw body 버퍼 (binary, MBCP TLV)
    floor_mbcp_buf: Vec<Vec<u8>>,
}

impl SignalingSession {
    /// Connect + IDENTIFY + ROOM_CREATE + ROOM_JOIN (첫 봇 — 방 생성).
    ///
    /// `ws_port`: hub WS 포트 (1974). `mode`: v3 에선 ROOM_CREATE 에 미전달
    /// (RoomMode 폐기 — duplex 는 트랙 단위). 시그니처 호환 위해 보존.
    pub async fn connect(
        server: &str,
        ws_port: u16,
        user_id: &str,
        room_name: &str,
        _mode: &str,
    ) -> Result<Self, DynErr> {
        let mut session = Self::open(server, ws_port, user_id).await?;

        // ROOM_CREATE (멱등 — 이미 있으면 기존 방)
        let create = session
            .request(OP_ROOM_CREATE, serde_json::json!({ "name": room_name }))
            .await?;
        if create.ok != Some(true) {
            return Err(format!("ROOM_CREATE failed: {:?}", create.d).into());
        }
        let room_id = create.d["room_id"]
            .as_str()
            .ok_or("ROOM_CREATE response missing room_id")?
            .to_string();
        info!("[sig] ROOM_CREATE ok room_id={}", room_id);

        session.do_join(&room_id).await?;
        Ok(session)
    }

    /// Connect + ROOM_CREATE(명시 room_id, 멱등 ensure) + ROOM_JOIN.
    /// capacity: 모든 봇이 동일 room_id 를 사전 합의 → publisher create, sub join.
    pub async fn connect_with_room_id(
        server: &str,
        ws_port: u16,
        user_id: &str,
        room_id: &str,
    ) -> Result<Self, DynErr> {
        let mut session = Self::open(server, ws_port, user_id).await?;
        // room_id 명시 → 멱등 ensure (room_ops: 이미 있으면 기존 방).
        let create = session
            .request(OP_ROOM_CREATE, serde_json::json!({
                "room_id": room_id,
                "name": room_id,
            }))
            .await?;
        if create.ok != Some(true) {
            return Err(format!("ROOM_CREATE(ensure) failed: {:?}", create.d).into());
        }
        session.do_join(room_id).await?;
        Ok(session)
    }

    /// Connect as subscriber to existing room (no ROOM_CREATE).
    pub async fn connect_to_room(
        server: &str,
        ws_port: u16,
        user_id: &str,
        room_id: &str,
    ) -> Result<Self, DynErr> {
        let mut session = Self::open(server, ws_port, user_id).await?;
        session.do_join(room_id).await?;
        Ok(session)
    }

    /// WS open + HELLO 수신 + IDENTIFY → IDENTIFY_RESULT.
    async fn open(server: &str, ws_port: u16, user_id: &str) -> Result<Self, DynErr> {
        // hub base_path = "/media" (system.toml). 클라 WS = /media/ws.
        let url = format!("ws://{}:{}/media/ws", server, ws_port);
        info!("[sig:{}] connecting to {}", user_id, url);

        let (ws, _) = connect_async(&url).await?;
        let mut session = Self {
            ws,
            pid: 1,
            user_id: user_id.to_string(),
            room_id: String::new(),
            server_config: None,
            event_buf: Vec::new(),
            floor_mbcp_buf: Vec::new(),
        };

        // 1) HELLO (서버 → 클라, handshake MSG)
        let hello = session.recv_frame().await?;
        if hello.header.op != OP_HELLO {
            return Err(format!("expected HELLO, got op=0x{:04X}", hello.header.op).into());
        }
        debug!("[sig:{}] HELLO received", user_id);

        // 2) IDENTIFY (pid=0, handshake) → IDENTIFY_RESULT
        session
            .send_msg(OP_IDENTIFY, 0, &serde_json::json!({
                "token": "oxlab",
                "user_id": user_id,
            }))
            .await?;

        loop {
            let rf = session.recv_frame().await?;
            if rf.header.op == OP_IDENTIFY_RESULT {
                let d: serde_json::Value =
                    serde_json::from_slice(&rf.body).unwrap_or(serde_json::Value::Null);
                let assigned = d["user_id"].as_str().unwrap_or(user_id).to_string();
                session.user_id = assigned.clone();
                info!("[sig] IDENTIFY ok user={}", assigned);
                break;
            }
            session.buffer_frame(rf);
        }

        Ok(session)
    }

    /// ROOM_JOIN → server_config 파싱.
    async fn do_join(&mut self, room_id: &str) -> Result<(), DynErr> {
        let resp = self
            .request(OP_ROOM_JOIN, serde_json::json!({ "room_id": room_id }))
            .await?;
        if resp.ok != Some(true) {
            return Err(format!("ROOM_JOIN failed: {:?}", resp.d).into());
        }

        let config = ServerConfig::parse(&resp.d);
        info!(
            "[sig] ROOM_JOIN ok room={} server={}:{} pub_ufrag={} sub_ufrag={}",
            room_id, config.server_ip, config.server_port, config.pub_ufrag, config.sub_ufrag,
        );

        self.room_id = room_id.to_string();
        self.server_config = Some(config);
        Ok(())
    }

    // ── 발행 ──

    /// PUBLISH_TRACKS — (kind, ssrc) 목록. 기본 intent (capacity 는 publish_intent 사용).
    pub async fn publish_tracks(&mut self, tracks: Vec<(String, u32)>) -> Result<(), DynErr> {
        let items: Vec<serde_json::Value> = tracks
            .iter()
            .map(|(kind, ssrc)| serde_json::json!({ "kind": kind, "ssrc": ssrc }))
            .collect();
        let resp = self
            .request_routed(OP_PUBLISH_TRACKS, serde_json::json!({ "tracks": items }))
            .await?;
        if resp.ok != Some(true) {
            return Err(format!("PUBLISH_TRACKS failed: {:?}", resp.d).into());
        }
        Ok(())
    }

    /// TRACKS_READY (구 TRACKS_ACK) — SDP renego 완료 통지 → SubscriberGate resume + PLI.
    /// 본문 = `{room_id, ssrcs:[]}` (oxrtc/oxe2e 정합). room_id 는 request_routed 주입.
    pub async fn tracks_ready(&mut self) -> Result<Packet, DynErr> {
        self.request_routed(OP_TRACKS_READY, serde_json::json!({ "ssrcs": [] }))
            .await
    }

    /// SUBSCRIBE_LAYER (0x1105) — simulcast 레이어 선택.
    pub async fn subscribe_layer(
        &mut self,
        targets: Vec<serde_json::Value>,
    ) -> Result<Packet, DynErr> {
        self.request_routed(OP_SUBSCRIBE_LAYER, serde_json::json!({ "targets": targets }))
            .await
    }

    /// sfud-routed Request — room_id 필수 계약(F13, 2026-06-10: "마지막 join 방" 힌트 폐기).
    /// hub 가 op body 의 room_id 로 sfu 라우팅. body 에 room_id 없으면 self.room_id 주입.
    pub async fn request_routed(
        &mut self,
        op: u16,
        mut d: serde_json::Value,
    ) -> Result<Packet, DynErr> {
        if let serde_json::Value::Object(ref mut m) = d {
            m.entry("room_id".to_string())
                .or_insert_with(|| serde_json::Value::String(self.room_id.clone()));
        }
        self.request(op, d).await
    }

    /// Heartbeat (keep WS alive) → ACK_OK.
    pub async fn heartbeat(&mut self) -> Result<(), DynErr> {
        let _ = self.request(OP_HEARTBEAT, serde_json::json!({})).await?;
        Ok(())
    }

    // ── Floor (v3: FLOOR_MBCP WS binary fallback) ──

    /// FLOOR_MBCP (0x2400) — MBCP TLV 를 WS binary 로 송신 (fire-and-forget).
    /// DC(SCTP) 셋업 부담 회피. 서버 응답(FTKN/FIDL/FRVK)은 FLOOR_MBCP 이벤트로 비동기 도착.
    pub async fn send_floor_mbcp(&mut self, mbcp: &[u8]) -> Result<(), DynErr> {
        let pid = self.next_pid();
        let frame = WireHeader::new_msg(OP_FLOOR_MBCP, pid).frame(mbcp);
        self.ws.send(Message::Binary(frame.into())).await?;
        Ok(())
    }

    /// 수신된 FLOOR_MBCP raw body (MBCP TLV) 전부 꺼냄. 봇이 mbcp::parse 로 해석.
    pub fn drain_floor_mbcp(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.floor_mbcp_buf)
    }

    // ── 이벤트 버퍼 ──

    pub fn drain_event(&mut self, op: u16) -> Option<Packet> {
        self.event_buf
            .iter()
            .position(|p| p.op == op)
            .map(|idx| self.event_buf.remove(idx))
    }

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

    /// non-blocking poll — 버퍼에 쌓인 WS 이벤트 수신 (Event ACK 자동 회신 포함).
    pub async fn poll_events(&mut self) {
        loop {
            match tokio::time::timeout(
                std::time::Duration::from_millis(1),
                self.recv_frame(),
            )
            .await
            {
                Ok(Ok(rf)) => self.buffer_frame(rf),
                _ => break,
            }
        }
    }

    /// 지정 timeout 내에 특정 op 이벤트 수신 대기.
    pub async fn wait_event(
        &mut self,
        op: u16,
        timeout: std::time::Duration,
    ) -> Option<Packet> {
        if let Some(pkt) = self.drain_event(op) {
            return Some(pkt);
        }
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return None;
            }
            match tokio::time::timeout(remaining, self.recv_frame()).await {
                Ok(Ok(rf)) => {
                    if rf.header.op == op {
                        return Some(frame_to_packet(&rf.header, &rf.body));
                    }
                    self.buffer_frame(rf);
                }
                Ok(Err(e)) => {
                    warn!("[sig] recv error while waiting op=0x{:04X}: {}", op, e);
                    return None;
                }
                Err(_) => return None,
            }
        }
    }

    pub async fn close(&mut self) {
        let _ = self.ws.close(None).await;
    }

    // ── internal ──

    fn next_pid(&mut self) -> u32 {
        let p = self.pid;
        self.pid += 1;
        p
    }

    /// 수신 프레임을 적절한 버퍼로 분류 (FLOOR_MBCP binary vs JSON 이벤트).
    fn buffer_frame(&mut self, rf: RecvFrame) {
        if rf.header.op == OP_FLOOR_MBCP {
            self.floor_mbcp_buf.push(rf.body);
        } else {
            self.event_buf.push(frame_to_packet(&rf.header, &rf.body));
        }
    }

    /// MSG frame (JSON body) 송신.
    async fn send_msg(
        &mut self,
        op: u16,
        pid: u32,
        d: &serde_json::Value,
    ) -> Result<(), DynErr> {
        let body = serde_json::to_vec(d)?;
        let frame = WireHeader::new_msg(op, pid).frame(&body);
        debug!("[sig] → op=0x{:04X} pid={}", op, pid);
        self.ws.send(Message::Binary(frame.into())).await?;
        Ok(())
    }

    /// 단일 WS Binary frame 수신.
    /// Event(0x2xxx, FLOOR_MBCP 제외) MSG 는 ACK_OK 자동 회신 (hub OutboundQueue 정합).
    async fn recv_frame(&mut self) -> Result<RecvFrame, DynErr> {
        loop {
            match self.ws.next().await {
                Some(Ok(Message::Binary(data))) => {
                    let (header, body) = split_frame(&data)?;
                    let body = body.to_vec();
                    debug!("[sig:{}] ← op=0x{:04X} ack={:?} len={}",
                        self.user_id, header.op, header.ack_state(), body.len());

                    // S→C Event = 클라 ACK 필수 (FLOOR_MBCP 는 no-ack 우회 경로)
                    let ack = header.ack_state().unwrap_or(WireAckState::Msg);
                    if ack == WireAckState::Msg
                        && opcode::is_event(header.op)
                        && header.op != OP_FLOOR_MBCP
                    {
                        let ack_frame =
                            WireHeader::new_ack(header.op, header.pid, WireAckState::AckOk)
                                .frame(&[]);
                        if let Err(e) = self.ws.send(Message::Binary(ack_frame.into())).await {
                            warn!("[sig] event ACK send failed op=0x{:04X}: {}", header.op, e);
                        }
                    }

                    return Ok(RecvFrame { header, body });
                }
                Some(Ok(Message::Close(_))) | None => return Err("ws closed".into()),
                Some(Ok(_)) => continue, // Text/Ping/Pong 무시 (v3 = binary only)
                Some(Err(e)) => return Err(format!("ws error: {e}").into()),
            }
        }
    }

    /// 요청 송신 + 동일 op ACK 응답 대기. 이벤트는 버퍼링, ERROR frame 도 반환.
    pub async fn request(
        &mut self,
        op: u16,
        d: serde_json::Value,
    ) -> Result<Packet, DynErr> {
        let pid = self.next_pid();
        self.send_msg(op, pid, &d).await?;

        loop {
            let rf = self.recv_frame().await?;
            // 동일 op + ack 응답
            if rf.header.op == op {
                if let Ok(state) = rf.header.ack_state() {
                    if state.is_ack() {
                        return Ok(frame_to_packet(&rf.header, &rf.body));
                    }
                }
            }
            // ERROR frame (요청 거부)
            if opcode::is_error(rf.header.op) {
                return Ok(frame_to_packet(&rf.header, &rf.body));
            }
            // 그 외 = 이벤트 → 버퍼
            self.buffer_frame(rf);
        }
    }
}

/// 수신 프레임 → application Packet (ack_state → ok 매핑, body → JSON d).
fn frame_to_packet(header: &WireHeader, body: &[u8]) -> Packet {
    let ok = match header.ack_state() {
        Ok(WireAckState::AckOk) => Some(true),
        Ok(WireAckState::AckFail) => Some(false),
        _ => None,
    };
    let d = if body.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(body).unwrap_or(serde_json::Value::Null)
    };
    Packet { op: header.op, pid: header.pid, ok, d }
}
