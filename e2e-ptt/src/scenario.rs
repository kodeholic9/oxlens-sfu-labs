// author: kodeholic (powered by Claude)
//! PTT E2E test scenarios — v2 (priority + queuing + preemption)
//!
//! Part 1: MBCP 시나리오 (기존 + v2 변경)
//!   1. basic_grant_release    — Idle → FREQ → FTKN → FREL → FIDL
//!   2. queued_when_busy       — A 발화 + B FREQ → B Queued (v2 큐잉)
//!   3. floor_switch           — A FREL → B FREQ → B FTKN
//!   4. rtp_gating             — 비발화자 RTP 차단 확인
//!
//! Part 2: WS 시나리오 (v2 신규)
//!   5. ws_priority_queuing    — A(pri=5) 발화 → B(pri=2) WS 요청 → Queued
//!   6. ws_preemption          — A(pri=2) 발화 → B(pri=5) WS 요청 → 선점
//!   7. ws_queue_pop_on_release— A 발화 + B 큐잉 → A release → B 자동 granted
//!   8. ws_queue_position      — A 발화 + B,C 큐잉 → 큐 위치 조회

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use oxlens_lab_common::media::{self, SrtpCtx};
use oxlens_lab_common::mbcp;
use oxlens_lab_common::signaling::{self, SignalingSession};

use crate::Args;

// ════════════════════════════════════════════════════════════
// MBCP Participant — full media pipeline
// ════════════════════════════════════════════════════════════

struct Participant {
    sig: SignalingSession,
    pub_socket: Arc<UdpSocket>,
    sub_socket: Arc<UdpSocket>,
    pub_srtp: SrtpCtx,
    sub_srtp: SrtpCtx,
    video_ssrc: u32,
    server_addr: SocketAddr,
    #[allow(dead_code)]
    user_id: String,
}

impl Participant {
    async fn send_mbcp(&mut self, pkt: &[u8]) -> Result<(), String> {
        let encrypted = self.pub_srtp.encrypt_rtcp(pkt)
            .ok_or("SRTCP encrypt failed")?;
        self.pub_socket.send_to(&encrypted, self.server_addr).await
            .map_err(|e| format!("send_mbcp: {e}"))?;
        Ok(())
    }

    async fn recv_mbcp_filtered(
        &mut self,
        filter_subtypes: &[u8],
        timeout: Duration,
    ) -> Option<mbcp::MbcpMessage> {
        let deadline = Instant::now() + timeout;
        let mut buf = vec![0u8; 2048];

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() { return None; }

            match tokio::time::timeout(remaining, self.sub_socket.recv_from(&mut buf)).await {
                Ok(Ok((n, _))) => {
                    let b1 = buf.get(1).copied().unwrap_or(0);
                    let pt = b1 & 0x7F;
                    let is_rtcp = (72..=79).contains(&pt);

                    if !is_rtcp { continue; }

                    let plaintext = match self.sub_srtp.decrypt_rtcp(&buf[..n]) {
                        Some(p) => p,
                        None => { debug!("SRTCP decrypt failed, len={}", n); continue; }
                    };

                    let msgs = mbcp::parse_compound(&plaintext);
                    for msg in msgs {
                        if filter_subtypes.contains(&msg.subtype) {
                            return Some(msg);
                        }
                        debug!("  skipping MBCP {} (waiting for {:?})",
                            msg.subtype_name(),
                            filter_subtypes.iter().map(|s| mbcp::subtype_name(*s)).collect::<Vec<_>>());
                    }
                }
                Ok(Err(_)) => continue,
                Err(_) => return None,
            }
        }
    }

    #[allow(dead_code)]
    async fn recv_mbcp(&mut self, timeout: Duration) -> Option<mbcp::MbcpMessage> {
        self.recv_mbcp_filtered(
            &[mbcp::SUBTYPE_FREQ, mbcp::SUBTYPE_FREL, mbcp::SUBTYPE_FTKN,
              mbcp::SUBTYPE_FIDL, mbcp::SUBTYPE_FRVK, mbcp::SUBTYPE_FPNG],
            timeout,
        ).await
    }

    async fn count_rtp_received(&mut self, timeout: Duration) -> u64 {
        let deadline = Instant::now() + timeout;
        let mut buf = vec![0u8; 2048];
        let mut count = 0u64;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() { return count; }

            match tokio::time::timeout(remaining, self.sub_socket.recv_from(&mut buf)).await {
                Ok(Ok((n, _))) => {
                    if media::is_rtp(buf[0]) && n >= 12 {
                        if self.sub_srtp.decrypt_rtp(&buf[..n]).is_some() {
                            count += 1;
                        }
                    }
                }
                Ok(Err(_)) => continue,
                Err(_) => return count,
            }
        }
    }

    async fn drain_recv(&mut self) {
        let mut buf = vec![0u8; 2048];
        loop {
            match tokio::time::timeout(Duration::from_millis(50), self.sub_socket.recv_from(&mut buf)).await {
                Ok(Ok(_)) => continue,
                _ => break,
            }
        }
    }

    async fn close(&mut self) {
        self.sig.close().await;
    }
}

async fn setup_participant(
    args: &Args,
    user_id: &str,
    room_id: Option<&str>,
    mode: &str,
    video_ssrc: u32,
) -> Result<Participant, String> {
    let sig = if let Some(rid) = room_id {
        SignalingSession::connect_to_room(&args.server, args.ws_port, user_id, rid)
            .await.map_err(|e| format!("connect_to_room: {e}"))?
    } else {
        SignalingSession::connect(&args.server, args.ws_port, user_id, &args.room, mode)
            .await.map_err(|e| format!("connect: {e}"))?
    };

    let sc = sig.server_config.clone().ok_or("no server_config")?;
    let server_addr: SocketAddr = format!("{}:{}", sc.server_ip, sc.server_port)
        .parse().map_err(|e| format!("parse addr: {e}"))?;

    let pub_socket = Arc::new(
        UdpSocket::bind("0.0.0.0:0").await.map_err(|e| format!("bind: {e}"))?
    );
    let (client_key, _server_key, client_salt, _server_salt) =
        media::setup_media_pc(&pub_socket, server_addr, &sc.pub_ufrag, &sc.pub_pwd, &format!("pub:{}", user_id))
            .await.map_err(|e| format!("pub setup: {e}"))?;

    let mut pub_srtp = SrtpCtx::new();
    pub_srtp.install(&client_key, &client_salt);

    let sub_socket = Arc::new(
        UdpSocket::bind("0.0.0.0:0").await.map_err(|e| format!("bind: {e}"))?
    );
    let (_ck, server_key, _cs, server_salt) =
        media::setup_media_pc(&sub_socket, server_addr, &sc.sub_ufrag, &sc.sub_pwd, &format!("sub:{}", user_id))
            .await.map_err(|e| format!("sub setup: {e}"))?;

    let mut sub_srtp = SrtpCtx::new();
    sub_srtp.install(&server_key, &server_salt);

    Ok(Participant {
        sig, pub_socket, sub_socket, pub_srtp, sub_srtp,
        video_ssrc, server_addr, user_id: user_id.to_string(),
    })
}

// ════════════════════════════════════════════════════════════
// WS-only Participant — signaling only, no media pipeline
// ════════════════════════════════════════════════════════════

struct WsParticipant {
    sig: SignalingSession,
    room_id: String,
}

impl WsParticipant {
    async fn floor_request(&mut self, priority: u8) -> Result<signaling::Packet, String> {
        self.sig.floor_request(&self.room_id, priority).await
            .map_err(|e| format!("floor_request: {e}"))
    }

    async fn floor_release(&mut self) -> Result<signaling::Packet, String> {
        self.sig.floor_release(&self.room_id).await
            .map_err(|e| format!("floor_release: {e}"))
    }

    async fn floor_queue_pos(&mut self) -> Result<signaling::Packet, String> {
        self.sig.floor_queue_pos(&self.room_id).await
            .map_err(|e| format!("floor_queue_pos: {e}"))
    }

    async fn wait_floor_taken(&mut self, timeout: Duration) -> Option<signaling::Packet> {
        self.sig.wait_event(signaling::OP_FLOOR_TAKEN, timeout).await
    }

    #[allow(dead_code)]
    async fn wait_floor_idle(&mut self, timeout: Duration) -> Option<signaling::Packet> {
        self.sig.wait_event(signaling::OP_FLOOR_IDLE, timeout).await
    }

    async fn wait_floor_revoke(&mut self, timeout: Duration) -> Option<signaling::Packet> {
        self.sig.wait_event(signaling::OP_FLOOR_REVOKE, timeout).await
    }

    /// queue pop에 의한 자동 Granted 패킷 수신 대기 (op=40, ok=true, pid=0)
    async fn wait_floor_granted_push(&mut self, timeout: Duration) -> Option<signaling::Packet> {
        self.sig.wait_event(signaling::OP_FLOOR_REQUEST, timeout).await
    }

    async fn close(&mut self) {
        self.sig.close().await;
    }
}

/// WS-only 참가자 셋업: WS 연결 + IDENTIFY + ROOM_CREATE/JOIN (미디어 없음)
async fn setup_ws_participant(
    args: &Args,
    user_id: &str,
    room_id: Option<&str>,
    mode: &str,
) -> Result<WsParticipant, String> {
    let sig = if let Some(rid) = room_id {
        SignalingSession::connect_to_room(&args.server, args.ws_port, user_id, rid)
            .await.map_err(|e| format!("connect_to_room: {e}"))?
    } else {
        SignalingSession::connect(&args.server, args.ws_port, user_id, &args.room, mode)
            .await.map_err(|e| format!("connect: {e}"))?
    };
    let room_id = sig.room_id.clone();
    Ok(WsParticipant { sig, room_id })
}

// ════════════════════════════════════════════════════════════
// Fake RTP builder
// ════════════════════════════════════════════════════════════

fn build_fake_rtp(ssrc: u32, seq: u16, total_size: usize, keyframe: bool) -> Vec<u8> {
    let size = total_size.max(16);
    let mut pkt = vec![0u8; size];
    pkt[0] = 0x80;
    pkt[1] = 96;
    if keyframe { pkt[1] |= 0x80; }
    pkt[2..4].copy_from_slice(&seq.to_be_bytes());
    pkt[4..8].copy_from_slice(&(seq as u32 * 3000).to_be_bytes());
    pkt[8..12].copy_from_slice(&ssrc.to_be_bytes());
    pkt[12] = 0x10; // VP8: S=1
    if keyframe {
        pkt[13] = 0x10; // keyframe + show_frame=1
    } else {
        pkt[13] = 0x11; // P-frame
    }
    pkt[14] = 0x00;
    pkt[15] = 0x00;
    pkt
}

// ════════════════════════════════════════════════════════════
// Part 1: MBCP Scenarios
// ════════════════════════════════════════════════════════════

// ── Test 1: basic_grant_release ──

pub async fn test_basic_grant_release(args: &Args) -> Result<(), String> {
    let mut a = setup_participant(args, "PTT_A", None, "ptt", 90001).await?;
    let room_id = a.sig.room_id.clone();
    let mut b = setup_participant(args, "PTT_B", Some(&room_id), "ptt", 90002).await?;

    a.sig.publish_tracks(vec![("video".to_string(), a.video_ssrc)]).await
        .map_err(|e| format!("A publish: {e}"))?;
    b.sig.publish_tracks(vec![("video".to_string(), b.video_ssrc)]).await
        .map_err(|e| format!("B publish: {e}"))?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    info!("  A → FREQ");
    a.send_mbcp(&mbcp::build_freq(a.video_ssrc)).await?;

    info!("  B waiting for FTKN...");
    let ftkn = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(3)).await
        .ok_or("B: FTKN timeout")?;
    info!("  B received FTKN, speaker={:?}", ftkn.data);

    let _ftkn_a = a.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(2)).await
        .ok_or("A: FTKN timeout")?;
    info!("  A received FTKN (self confirm)");

    info!("  A → FREL");
    a.send_mbcp(&mbcp::build_frel(a.video_ssrc)).await?;

    info!("  B waiting for FIDL...");
    let fidl = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FIDL], Duration::from_secs(3)).await
        .ok_or("B: FIDL timeout")?;
    info!("  B received FIDL, prev_speaker={:?}", fidl.data);

    a.close().await;
    b.close().await;
    Ok(())
}

// ── Test 2: queued_when_busy (v2 — 기존 deny_when_busy에서 변경) ──

pub async fn test_queued_when_busy(args: &Args) -> Result<(), String> {
    let mut a = setup_participant(args, "Q_A", None, "ptt", 91001).await?;
    let room_id = a.sig.room_id.clone();
    let mut b = setup_participant(args, "Q_B", Some(&room_id), "ptt", 91002).await?;

    a.sig.publish_tracks(vec![("video".to_string(), a.video_ssrc)]).await
        .map_err(|e| format!("{e}"))?;
    b.sig.publish_tracks(vec![("video".to_string(), b.video_ssrc)]).await
        .map_err(|e| format!("{e}"))?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    // A takes floor
    info!("  A → FREQ");
    a.send_mbcp(&mbcp::build_freq(a.video_ssrc)).await?;
    let _ = a.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(3)).await
        .ok_or("A: FTKN timeout")?;
    let _ = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(2)).await;

    // B tries FREQ (MBCP pri=0, A도 pri=0 → 동일 우선순위 → v2 큐잉)
    info!("  B → FREQ (should be queued)");
    b.send_mbcp(&mbcp::build_freq(b.video_ssrc)).await?;

    // B should receive FRVK with "queued:" message (MBCP에서 큐잉은 FRVK로 전달)
    let frvk = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FRVK], Duration::from_secs(3)).await
        .ok_or("B: FRVK/queued timeout")?;

    let data = frvk.data.as_deref().unwrap_or("");
    if !data.contains("queued:") {
        return Err(format!("B: FRVK data should contain 'queued:', got: {}", data));
    }
    info!("  B received FRVK(queued): {}", data);

    // Cleanup
    a.send_mbcp(&mbcp::build_frel(a.video_ssrc)).await?;
    // B가 큐에 있었으므로 A release 후 B가 자동 grant → FTKN 수신 가능
    let _ = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(2)).await;
    // B도 release
    b.send_mbcp(&mbcp::build_frel(b.video_ssrc)).await?;

    a.close().await;
    b.close().await;
    Ok(())
}

// ── Test 3: floor_switch ──

pub async fn test_floor_switch(args: &Args) -> Result<(), String> {
    let mut a = setup_participant(args, "SW_A", None, "ptt", 92001).await?;
    let room_id = a.sig.room_id.clone();
    let mut b = setup_participant(args, "SW_B", Some(&room_id), "ptt", 92002).await?;

    a.sig.publish_tracks(vec![("video".to_string(), a.video_ssrc)]).await
        .map_err(|e| format!("{e}"))?;
    b.sig.publish_tracks(vec![("video".to_string(), b.video_ssrc)]).await
        .map_err(|e| format!("{e}"))?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    a.send_mbcp(&mbcp::build_freq(a.video_ssrc)).await?;
    let _ = a.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(3)).await;
    let _ = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(2)).await;

    info!("  A → FREL");
    a.send_mbcp(&mbcp::build_frel(a.video_ssrc)).await?;
    let _ = a.recv_mbcp_filtered(&[mbcp::SUBTYPE_FIDL], Duration::from_secs(2)).await;
    let _ = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FIDL], Duration::from_secs(2)).await;

    info!("  B → FREQ");
    b.send_mbcp(&mbcp::build_freq(b.video_ssrc)).await?;

    let ftkn = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(3)).await
        .ok_or("B: FTKN timeout after floor switch")?;
    info!("  B received FTKN (floor switch OK), speaker={:?}", ftkn.data);

    b.send_mbcp(&mbcp::build_frel(b.video_ssrc)).await?;

    a.close().await;
    b.close().await;
    Ok(())
}

// ── Test 4: rtp_gating ──

pub async fn test_rtp_gating(args: &Args) -> Result<(), String> {
    let mut a = setup_participant(args, "GATE_A", None, "ptt", 93001).await?;
    let room_id = a.sig.room_id.clone();
    let mut b = setup_participant(args, "GATE_B", Some(&room_id), "ptt", 93002).await?;

    a.sig.publish_tracks(vec![("video".to_string(), a.video_ssrc)]).await
        .map_err(|e| format!("{e}"))?;
    b.sig.publish_tracks(vec![("video".to_string(), b.video_ssrc)]).await
        .map_err(|e| format!("{e}"))?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    info!("  B sends 10 RTP without floor (should be gated)");
    for seq in 0..10u16 {
        let rtp = build_fake_rtp(b.video_ssrc, seq, 200, seq == 0);
        if let Some(encrypted) = b.pub_srtp.encrypt_rtp(&rtp) {
            let _ = b.pub_socket.send_to(&encrypted, b.server_addr).await;
        }
    }

    let gated_count = a.count_rtp_received(Duration::from_secs(1)).await;
    info!("  A received {} RTP (expect 0)", gated_count);
    if gated_count > 0 {
        return Err(format!("RTP gating failed: A received {} packets without floor holder", gated_count));
    }

    a.send_mbcp(&mbcp::build_freq(a.video_ssrc)).await?;
    let _ = a.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(3)).await;
    b.drain_recv().await;

    info!("  A sends 10 RTP with floor (should pass, first=keyframe)");
    for seq in 0..10u16 {
        let rtp = build_fake_rtp(a.video_ssrc, seq, 200, seq == 0);
        if let Some(encrypted) = a.pub_srtp.encrypt_rtp(&rtp) {
            let _ = a.pub_socket.send_to(&encrypted, a.server_addr).await;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let passed_count = b.count_rtp_received(Duration::from_secs(2)).await;
    info!("  B received {} RTP (expect > 0)", passed_count);
    if passed_count == 0 {
        return Err("RTP gating failed: B received 0 packets when A has floor".into());
    }

    a.send_mbcp(&mbcp::build_frel(a.video_ssrc)).await?;

    a.close().await;
    b.close().await;
    Ok(())
}

// ════════════════════════════════════════════════════════════
// Part 2: WS Floor Control v2 Scenarios
// ════════════════════════════════════════════════════════════

// ── Test 5: ws_priority_queuing ──
// A(pri=5) 발화 → B(pri=2) WS 요청 → Queued 응답

pub async fn test_ws_priority_queuing(args: &Args) -> Result<(), String> {
    let mut a = setup_ws_participant(args, "WQ_A", None, "ptt").await?;
    let room_id = a.room_id.clone();
    let mut b = setup_ws_participant(args, "WQ_B", Some(&room_id), "ptt").await?;

    // A gets floor (pri=5)
    let resp = a.floor_request(5).await?;
    if resp.d.get("granted").and_then(|v| v.as_bool()) != Some(true) {
        return Err(format!("A: expected granted, got {:?}", resp.d));
    }
    info!("  A granted (pri=5)");

    // Drain B's FLOOR_TAKEN event
    let _ = b.wait_floor_taken(Duration::from_secs(2)).await;

    // B requests floor (pri=2, < 5 → queued)
    let resp = b.floor_request(2).await?;
    let queued = resp.d.get("queued").and_then(|v| v.as_bool()).unwrap_or(false);
    if !queued {
        return Err(format!("B: expected queued=true, got {:?}", resp.d));
    }
    let position = resp.d.get("position").and_then(|v| v.as_u64()).unwrap_or(0);
    let priority = resp.d.get("priority").and_then(|v| v.as_u64()).unwrap_or(0);
    info!("  B queued: position={}, priority={}", position, priority);

    if position != 1 {
        return Err(format!("B: expected position=1, got {}", position));
    }
    if priority != 2 {
        return Err(format!("B: expected priority=2, got {}", priority));
    }

    // Cleanup
    a.floor_release().await?;
    // B auto-granted after A release → drain events
    let _ = b.wait_floor_taken(Duration::from_secs(2)).await;
    b.floor_release().await?;

    a.close().await;
    b.close().await;
    Ok(())
}

// ── Test 6: ws_preemption ──
// A(pri=2) 발화 → B(pri=5) 선점 → A revoked, B granted

pub async fn test_ws_preemption(args: &Args) -> Result<(), String> {
    let mut a = setup_ws_participant(args, "WP_A", None, "ptt").await?;
    let room_id = a.room_id.clone();
    let mut b = setup_ws_participant(args, "WP_B", Some(&room_id), "ptt").await?;

    // A gets floor (pri=2)
    let resp = a.floor_request(2).await?;
    if resp.d.get("granted").and_then(|v| v.as_bool()) != Some(true) {
        return Err(format!("A: expected granted, got {:?}", resp.d));
    }
    info!("  A granted (pri=2)");

    // Drain B's FLOOR_TAKEN
    let _ = b.wait_floor_taken(Duration::from_secs(2)).await;

    // B requests floor (pri=5, > 2 → preemption)
    let resp = b.floor_request(5).await?;
    let granted = resp.d.get("granted").and_then(|v| v.as_bool()).unwrap_or(false);
    if !granted {
        return Err(format!("B: expected granted=true (preemption), got {:?}", resp.d));
    }
    let speaker = resp.d.get("speaker").and_then(|v| v.as_str()).unwrap_or("");
    info!("  B granted by preemption (pri=5), speaker={}", speaker);

    // A should receive FLOOR_REVOKE
    let revoke = a.wait_floor_revoke(Duration::from_secs(2)).await
        .ok_or("A: FLOOR_REVOKE timeout")?;
    let cause = revoke.d.get("cause").and_then(|v| v.as_str()).unwrap_or("");
    if cause != "preempted" {
        return Err(format!("A: expected cause='preempted', got '{}'", cause));
    }
    info!("  A received FLOOR_REVOKE (cause=preempted)");

    // A should also receive FLOOR_TAKEN (new speaker=B)
    let taken = a.wait_floor_taken(Duration::from_secs(2)).await
        .ok_or("A: FLOOR_TAKEN timeout after preemption")?;
    let new_speaker = taken.d.get("speaker").and_then(|v| v.as_str()).unwrap_or("");
    info!("  A received FLOOR_TAKEN (speaker={})", new_speaker);

    // Cleanup
    b.floor_release().await?;

    a.close().await;
    b.close().await;
    Ok(())
}

// ── Test 7: ws_queue_pop_on_release ──
// A(pri=5) 발화 + B(pri=2) 큐잉 → A release → B 자동 granted

pub async fn test_ws_queue_pop_on_release(args: &Args) -> Result<(), String> {
    let mut a = setup_ws_participant(args, "WR_A", None, "ptt").await?;
    let room_id = a.room_id.clone();
    let mut b = setup_ws_participant(args, "WR_B", Some(&room_id), "ptt").await?;

    // A gets floor (pri=5)
    a.floor_request(5).await?;
    info!("  A granted (pri=5)");
    let _ = b.wait_floor_taken(Duration::from_secs(2)).await;

    // B requests (pri=2 → queued)
    let resp = b.floor_request(2).await?;
    if resp.d.get("queued").and_then(|v| v.as_bool()) != Some(true) {
        return Err(format!("B: expected queued, got {:?}", resp.d));
    }
    info!("  B queued (pri=2)");

    // A releases → server should auto-grant B
    info!("  A → floor_release");
    a.floor_release().await?;

    // B should receive FLOOR_TAKEN (speaker=WR_B, from queue pop broadcast)
    let taken = b.wait_floor_taken(Duration::from_secs(3)).await
        .ok_or("B: FLOOR_TAKEN timeout (queue pop)")?;
    let speaker = taken.d.get("speaker").and_then(|v| v.as_str()).unwrap_or("");
    info!("  B received FLOOR_TAKEN (speaker={}) — queue pop OK", speaker);

    if !speaker.contains("WR_B") {
        return Err(format!("B: expected speaker containing 'WR_B', got '{}'", speaker));
    }

    // B should also receive auto-granted response (op=40, ok=true, pid=0)
    let granted = b.wait_floor_granted_push(Duration::from_secs(2)).await;
    if let Some(pkt) = &granted {
        info!("  B received auto-granted push: {:?}", pkt.d);
    } else {
        warn!("  B: auto-granted push not received (non-critical)");
    }

    // Cleanup
    b.floor_release().await?;

    a.close().await;
    b.close().await;
    Ok(())
}

// ── Test 8: ws_queue_position ──
// A(pri=10) 발화 + B(pri=2) + C(pri=5) 큐잉 → 큐 순서: C(1st), B(2nd)

pub async fn test_ws_queue_position(args: &Args) -> Result<(), String> {
    let mut a = setup_ws_participant(args, "QP_A", None, "ptt").await?;
    let room_id = a.room_id.clone();
    let mut b = setup_ws_participant(args, "QP_B", Some(&room_id), "ptt").await?;
    let mut c = setup_ws_participant(args, "QP_C", Some(&room_id), "ptt").await?;

    // A gets floor (pri=10)
    a.floor_request(10).await?;
    info!("  A granted (pri=10)");
    let _ = b.wait_floor_taken(Duration::from_secs(2)).await;
    let _ = c.wait_floor_taken(Duration::from_secs(2)).await;

    // B requests (pri=2 → queued)
    let resp = b.floor_request(2).await?;
    let b_pos = resp.d.get("position").and_then(|v| v.as_u64()).unwrap_or(0);
    info!("  B queued: position={}", b_pos);

    // C requests (pri=5 → queued, 우선순위 높으므로 B 앞에)
    let resp = c.floor_request(5).await?;
    let c_pos = resp.d.get("position").and_then(|v| v.as_u64()).unwrap_or(0);
    info!("  C queued: position={}", c_pos);

    if c_pos != 1 {
        return Err(format!("C: expected position=1 (higher priority), got {}", c_pos));
    }

    // C queries queue_pos
    let resp = c.floor_queue_pos().await?;
    let qpos = resp.d.get("position").and_then(|v| v.as_u64()).unwrap_or(99);
    let qpri = resp.d.get("priority").and_then(|v| v.as_u64()).unwrap_or(99);
    info!("  C queue_pos: position={}, priority={}", qpos, qpri);

    if qpos != 1 || qpri != 5 {
        return Err(format!("C: expected pos=1/pri=5, got pos={}/pri={}", qpos, qpri));
    }

    // B queries queue_pos
    let resp = b.floor_queue_pos().await?;
    let bpos = resp.d.get("position").and_then(|v| v.as_u64()).unwrap_or(99);
    info!("  B queue_pos: position={}", bpos);

    if bpos != 2 {
        return Err(format!("B: expected pos=2, got {}", bpos));
    }

    // Cleanup: A release → C auto-grant → C release → B auto-grant → B release
    a.floor_release().await?;
    let _ = c.wait_floor_taken(Duration::from_secs(2)).await;
    c.floor_release().await?;
    let _ = b.wait_floor_taken(Duration::from_secs(2)).await;
    b.floor_release().await?;

    a.close().await;
    b.close().await;
    c.close().await;
    Ok(())
}
