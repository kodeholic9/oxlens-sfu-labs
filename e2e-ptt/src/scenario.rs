// author: kodeholic (powered by Claude)
//! PTT E2E test scenarios
//!
//! 각 시나리오는 독립적으로 room을 생성하고, 참가자를 셋업하고, 검증 후 정리한다.
//! MBCP 패킷은 publish PC의 SRTCP 채널로 송수신한다.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use oxlens_lab_common::media::{self, SrtpCtx};
use oxlens_lab_common::mbcp;
use oxlens_lab_common::signaling::SignalingSession;

use crate::Args;

// ────────────────────────────────────────────────────────────
// Participant setup helper
// ────────────────────────────────────────────────────────────

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
    /// SRTCP로 MBCP APP 패킷 전송 (publish PC)
    async fn send_mbcp(&mut self, pkt: &[u8]) -> Result<(), String> {
        let encrypted = self.pub_srtp.encrypt_rtcp(pkt)
            .ok_or("SRTCP encrypt failed")?;
        self.pub_socket.send_to(&encrypted, self.server_addr).await
            .map_err(|e| format!("send_mbcp: {e}"))?;
        Ok(())
    }

    /// Subscribe PC에서 특정 subtype의 MBCP 수신 대기 (timeout 내)
    ///
    /// 서버가 relay_publish_rtcp에서 MBCP를 중복 전달하는 케이스가 있으므로,
    /// 원하는 subtype이 아닌 MBCP는 스킵한다.
    /// `filter_subtypes`: 이 subtype 중 하나가 오면 반환, 나머지는 무시
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

                    if !is_rtcp {
                        continue;
                    }

                    let plaintext = match self.sub_srtp.decrypt_rtcp(&buf[..n]) {
                        Some(p) => p,
                        None => {
                            debug!("SRTCP decrypt failed, len={}", n);
                            continue;
                        }
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

    /// Subscribe PC에서 아무 MBCP 수신 대기
    async fn recv_mbcp(&mut self, timeout: Duration) -> Option<mbcp::MbcpMessage> {
        self.recv_mbcp_filtered(
            &[mbcp::SUBTYPE_FREQ, mbcp::SUBTYPE_FREL, mbcp::SUBTYPE_FTKN,
              mbcp::SUBTYPE_FIDL, mbcp::SUBTYPE_FRVK, mbcp::SUBTYPE_FPNG],
            timeout,
        ).await
    }

    /// Subscribe PC에서 RTP 수신 시도 (timeout 내 패킷 수 카운트)
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
                    // RTCP 패킷은 무시 (MBCP 에코 등)
                }
                Ok(Err(_)) => continue,
                Err(_) => return count,
            }
        }
    }

    /// 수신 버퍼 비우기 (이전 시나리오의 잔여 패킷 제거)
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

/// 참가자 1명 셋업: WS 연결 + STUN + DTLS + SRTP (publish + subscribe PC)
async fn setup_participant(
    args: &Args,
    user_id: &str,
    room_id: Option<&str>,
    mode: &str,
    video_ssrc: u32,
) -> Result<Participant, String> {
    let sig = if let Some(rid) = room_id {
        SignalingSession::connect_to_room(&args.server, args.ws_port, user_id, rid)
            .await
            .map_err(|e| format!("connect_to_room: {e}"))?
    } else {
        SignalingSession::connect(&args.server, args.ws_port, user_id, &args.room, mode)
            .await
            .map_err(|e| format!("connect: {e}"))?
    };

    let sc = sig.server_config.clone().ok_or("no server_config")?;
    let server_addr: SocketAddr = format!("{}:{}", sc.server_ip, sc.server_port)
        .parse()
        .map_err(|e| format!("parse addr: {e}"))?;

    let pub_socket = Arc::new(
        UdpSocket::bind("0.0.0.0:0").await.map_err(|e| format!("bind: {e}"))?
    );
    let (client_key, _server_key, client_salt, _server_salt) =
        media::setup_media_pc(&pub_socket, server_addr, &sc.pub_ufrag, &sc.pub_pwd, &format!("pub:{}", user_id))
            .await
            .map_err(|e| format!("pub setup: {e}"))?;

    let mut pub_srtp = SrtpCtx::new();
    pub_srtp.install(&client_key, &client_salt);

    let sub_socket = Arc::new(
        UdpSocket::bind("0.0.0.0:0").await.map_err(|e| format!("bind: {e}"))?
    );
    let (_ck, server_key, _cs, server_salt) =
        media::setup_media_pc(&sub_socket, server_addr, &sc.sub_ufrag, &sc.sub_pwd, &format!("sub:{}", user_id))
            .await
            .map_err(|e| format!("sub setup: {e}"))?;

    let mut sub_srtp = SrtpCtx::new();
    sub_srtp.install(&server_key, &server_salt);

    Ok(Participant {
        sig,
        pub_socket,
        sub_socket,
        pub_srtp,
        sub_srtp,
        video_ssrc,
        server_addr,
        user_id: user_id.to_string(),
    })
}

/// Fake RTP — VP8 키프레임 마커 포함
///
/// PTT rewriter가 `keyframe_wait=true` 상태에서 키프레임이 아닌 패킷을 드롭하므로,
/// 첫 패킷은 VP8 키프레임으로 만들어야 한다.
///
/// VP8 payload descriptor (1바이트 최소):
///   byte 0: |X|R|N|S|R|PID(3)|
///   S=1, PID=0 이면 키프레임 시작 (partition 0)
///
/// VP8 uncompressed data header (keyframe only):
///   bytes 0-2: frame tag (keyframe: size(19bit) << 5 | show_frame(1) << 4 | version(3) << 1 | frame_type(0))
///   frame_type=0 → keyframe
fn build_fake_rtp(ssrc: u32, seq: u16, total_size: usize, keyframe: bool) -> Vec<u8> {
    let size = total_size.max(16); // RTP(12) + VP8 descriptor(1) + VP8 header(3) 최소
    let mut pkt = vec![0u8; size];
    // RTP header
    pkt[0] = 0x80;
    pkt[1] = 96; // PT=96 (VP8)
    if keyframe {
        pkt[1] |= 0x80; // marker bit
    }
    pkt[2..4].copy_from_slice(&seq.to_be_bytes());
    pkt[4..8].copy_from_slice(&(seq as u32 * 3000).to_be_bytes());
    pkt[8..12].copy_from_slice(&ssrc.to_be_bytes());

    // VP8 payload descriptor (offset 12)
    // S=1, PID=0: partition 0 시작
    pkt[12] = 0x10; // |0|0|0|1|0|000| → S=1

    // VP8 uncompressed data header (offset 13, keyframe only)
    if keyframe {
        // frame tag: frame_type=0 (keyframe), version=0, show_frame=1, size는 0으로
        // byte 0: size[0:2] | show_frame | version[0:2] | frame_type
        // = 0b_000_1_000_0 = 0x10 → but VP8 spec: LSB first
        // frame_type=0 (1bit), version=0 (3bit), show_frame=1 (1bit), first_part_size=0 (19bit)
        // byte0 = frame_type(0) | version(000) | show_frame(1) << 4 = 0x10
        pkt[13] = 0x10; // keyframe + show_frame=1
        pkt[14] = 0x00;
        pkt[15] = 0x00;
    } else {
        // frame_type=1 (not keyframe)
        pkt[13] = 0x11; // P-frame
        pkt[14] = 0x00;
        pkt[15] = 0x00;
    }

    pkt
}

// ════════════════════════════════════════════════════════════
// Test 1: basic_grant_release
// ════════════════════════════════════════════════════════════

pub async fn test_basic_grant_release(args: &Args) -> Result<(), String> {
    let mut a = setup_participant(args, "PTT_A", None, "ptt", 90001).await?;
    let room_id = a.sig.room_id.clone();
    let mut b = setup_participant(args, "PTT_B", Some(&room_id), "ptt", 90002).await?;

    a.sig.publish_tracks(vec![("video".to_string(), a.video_ssrc)]).await
        .map_err(|e| format!("A publish: {e}"))?;
    b.sig.publish_tracks(vec![("video".to_string(), b.video_ssrc)]).await
        .map_err(|e| format!("B publish: {e}"))?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    // === A sends FREQ ===
    info!("  A → FREQ");
    a.send_mbcp(&mbcp::build_freq(a.video_ssrc)).await?;

    // === B should receive FTKN ===
    info!("  B waiting for FTKN...");
    let ftkn = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(3)).await
        .ok_or("B: FTKN timeout")?;
    info!("  B received FTKN, speaker={:?}", ftkn.data);

    // === A also gets FTKN ===
    let ftkn_a = a.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(2)).await
        .ok_or("A: FTKN timeout")?;
    info!("  A received FTKN (self confirm)");

    // === A sends FREL ===
    info!("  A → FREL");
    a.send_mbcp(&mbcp::build_frel(a.video_ssrc)).await?;

    // === B should receive FIDL ===
    info!("  B waiting for FIDL...");
    let fidl = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FIDL], Duration::from_secs(3)).await
        .ok_or("B: FIDL timeout")?;
    info!("  B received FIDL, prev_speaker={:?}", fidl.data);

    a.close().await;
    b.close().await;
    Ok(())
}

// ════════════════════════════════════════════════════════════
// Test 2: deny_when_busy
// ════════════════════════════════════════════════════════════

pub async fn test_deny_when_busy(args: &Args) -> Result<(), String> {
    let mut a = setup_participant(args, "DENY_A", None, "ptt", 91001).await?;
    let room_id = a.sig.room_id.clone();
    let mut b = setup_participant(args, "DENY_B", Some(&room_id), "ptt", 91002).await?;

    a.sig.publish_tracks(vec![("video".to_string(), a.video_ssrc)]).await
        .map_err(|e| format!("{e}"))?;
    b.sig.publish_tracks(vec![("video".to_string(), b.video_ssrc)]).await
        .map_err(|e| format!("{e}"))?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    // A takes floor
    info!("  A → FREQ");
    a.send_mbcp(&mbcp::build_freq(a.video_ssrc)).await?;

    // A waits FTKN
    let _ = a.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(3)).await
        .ok_or("A: FTKN timeout")?;

    // Drain B's FTKN
    let _ = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(2)).await;

    // B tries to take floor (should be denied)
    info!("  B → FREQ (should be denied)");
    b.send_mbcp(&mbcp::build_freq(b.video_ssrc)).await?;

    // B should receive FRVK with deny reason
    let frvk = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FRVK], Duration::from_secs(3)).await
        .ok_or("B: FRVK/deny timeout")?;

    let reason = frvk.data.as_deref().unwrap_or("");
    if !reason.contains("denied") {
        return Err(format!("B: FRVK data should contain 'denied', got: {}", reason));
    }
    info!("  B received FRVK(denied): {}", reason);

    // Cleanup
    a.send_mbcp(&mbcp::build_frel(a.video_ssrc)).await?;

    a.close().await;
    b.close().await;
    Ok(())
}

// ════════════════════════════════════════════════════════════
// Test 3: floor_switch
// ════════════════════════════════════════════════════════════

pub async fn test_floor_switch(args: &Args) -> Result<(), String> {
    let mut a = setup_participant(args, "SW_A", None, "ptt", 92001).await?;
    let room_id = a.sig.room_id.clone();
    let mut b = setup_participant(args, "SW_B", Some(&room_id), "ptt", 92002).await?;

    a.sig.publish_tracks(vec![("video".to_string(), a.video_ssrc)]).await
        .map_err(|e| format!("{e}"))?;
    b.sig.publish_tracks(vec![("video".to_string(), b.video_ssrc)]).await
        .map_err(|e| format!("{e}"))?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    // A takes floor
    a.send_mbcp(&mbcp::build_freq(a.video_ssrc)).await?;
    let _ = a.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(3)).await;
    let _ = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(2)).await;

    // A releases
    info!("  A → FREL");
    a.send_mbcp(&mbcp::build_frel(a.video_ssrc)).await?;

    // Wait for FIDL propagation
    let _ = a.recv_mbcp_filtered(&[mbcp::SUBTYPE_FIDL], Duration::from_secs(2)).await;
    let _ = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FIDL], Duration::from_secs(2)).await;

    // B takes floor
    info!("  B → FREQ");
    b.send_mbcp(&mbcp::build_freq(b.video_ssrc)).await?;

    // B should get FTKN
    let ftkn = b.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(3)).await
        .ok_or("B: FTKN timeout after floor switch")?;
    info!("  B received FTKN (floor switch OK), speaker={:?}", ftkn.data);

    // Cleanup
    b.send_mbcp(&mbcp::build_frel(b.video_ssrc)).await?;

    a.close().await;
    b.close().await;
    Ok(())
}

// ════════════════════════════════════════════════════════════
// Test 4: rtp_gating
// ════════════════════════════════════════════════════════════

pub async fn test_rtp_gating(args: &Args) -> Result<(), String> {
    let mut a = setup_participant(args, "GATE_A", None, "ptt", 93001).await?;
    let room_id = a.sig.room_id.clone();
    let mut b = setup_participant(args, "GATE_B", Some(&room_id), "ptt", 93002).await?;

    a.sig.publish_tracks(vec![("video".to_string(), a.video_ssrc)]).await
        .map_err(|e| format!("{e}"))?;
    b.sig.publish_tracks(vec![("video".to_string(), b.video_ssrc)]).await
        .map_err(|e| format!("{e}"))?;

    tokio::time::sleep(Duration::from_millis(300)).await;

    // Nobody has floor. B sends RTP — A should NOT receive it.
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

    // Now A takes floor
    a.send_mbcp(&mbcp::build_freq(a.video_ssrc)).await?;
    let _ = a.recv_mbcp_filtered(&[mbcp::SUBTYPE_FTKN], Duration::from_secs(3)).await;
    // B의 subscribe 소켓에도 FTKN 오므로 drain
    b.drain_recv().await;

    // A sends RTP — B should receive it
    // 첫 패킷은 VP8 키프레임이어야 PTT rewriter가 통과시킴
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

    // Cleanup
    a.send_mbcp(&mbcp::build_frel(a.video_ssrc)).await?;

    a.close().await;
    b.close().await;
    Ok(())
}
