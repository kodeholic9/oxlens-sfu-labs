// author: kodeholic (powered by Claude)
//! Conference mode benchmark — all publish, all subscribe
//!
//! N participants join one room. Each participant has:
//!   - Publish PC:    STUN → DTLS → SRTP → fake RTP send @ fps
//!   - Subscribe PC:  STUN → DTLS → SRTP → receive from N-1 others
//!
//! Measures: per-participant tx/rx, per-sender loss, E2E latency
//! Total streams: N × (N-1)

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::{Instant, interval};
use tracing::{info, warn};

use crate::signaling::SignalingSession;
use crate::media::{SrtpCtx, setup_media_pc, is_rtp, now_micros};
use crate::Args;

// ═══════════════════════════════════════════════════════════
// Result types
// ═══════════════════════════════════════════════════════════

#[derive(Debug, Default)]
pub struct ConferenceResult {
    pub duration_secs: f64,
    pub num_participants: u32,
    pub total_streams: u32,         // N × (N-1)
    // aggregate
    pub total_tx: u64,
    pub total_rx: u64,
    pub total_lost: u64,
    pub loss_rate: f64,
    pub input_pps: f64,             // total tx / duration
    pub output_pps: f64,            // total rx / duration
    pub input_mbps: f64,
    pub output_mbps: f64,
    pub latency_avg_us: f64,
    pub latency_p95_us: f64,
    pub latency_max_us: f64,
    // per participant
    pub participants: Vec<ParticipantResult>,
}

#[derive(Debug, Default, Clone)]
pub struct ParticipantResult {
    pub id: String,
    pub tx_packets: u64,
    pub rx_packets: u64,
    pub rx_lost: u64,
    pub rx_from_count: u32,         // distinct senders received
    pub latency_avg_us: f64,
    pub latency_p95_us: f64,
    pub latency_max_us: f64,
}

// ═══════════════════════════════════════════════════════════
// RTP builder (conference: sender_id in payload)
// ═══════════════════════════════════════════════════════════
//
// Payload layout:
//   bytes 0-11:  RTP header (V=2, PT=96, seq, timestamp, ssrc)
//   bytes 12-13: sender_id (u16 LE)
//   bytes 14-21: send_timestamp (u64 LE, microseconds)
//   bytes 22+:   zero padding

fn build_conference_rtp(ssrc: u32, seq: u16, timestamp: u32, total_size: usize, sender_id: u16) -> Vec<u8> {
    let payload_size = if total_size > 12 { total_size - 12 } else { 0 };
    let mut pkt = Vec::with_capacity(total_size);

    // RTP header (12 bytes)
    pkt.push(0x80); // V=2
    pkt.push(96);   // PT=96 (VP8)
    pkt.extend_from_slice(&seq.to_be_bytes());
    pkt.extend_from_slice(&timestamp.to_be_bytes());
    pkt.extend_from_slice(&ssrc.to_be_bytes());

    // Payload: sender_id(2B) + timestamp(8B) + padding
    if payload_size >= 10 {
        pkt.extend_from_slice(&sender_id.to_le_bytes());
        let ts = now_micros();
        pkt.extend_from_slice(&ts.to_le_bytes());
        pkt.resize(12 + payload_size, 0x00);
    } else {
        pkt.resize(12 + payload_size, 0x00);
    }

    pkt
}

// ═══════════════════════════════════════════════════════════
// Per-sender tracking (in recv loop)
// ═══════════════════════════════════════════════════════════

struct SenderTracker {
    expected_seq: Option<u16>,
    rx_packets: u64,
    lost: u64,
    latencies: Vec<f64>,
}

impl SenderTracker {
    fn new() -> Self {
        Self { expected_seq: None, rx_packets: 0, lost: 0, latencies: Vec::new() }
    }
}

// recv loop return type
struct RecvResult {
    rx_packets: u64,
    rx_lost: u64,
    from_count: u32,
    latencies: Vec<f64>,
}

// ═══════════════════════════════════════════════════════════
// Send loop (per participant)
// ═══════════════════════════════════════════════════════════

async fn conference_send_loop(
    id: String,
    socket: Arc<UdpSocket>,
    mut srtp: SrtpCtx,
    mut sig: SignalingSession,
    ssrc: u32,
    sender_id: u16,
    server_addr: SocketAddr,
    fps: u32,
    pkt_size: usize,
    deadline: Instant,
) -> u64 {
    let frame_interval = Duration::from_micros(1_000_000 / fps as u64);
    let mut ticker = interval(frame_interval);
    let ts_increment = 90000 / fps;

    let mut seq: u16 = 0;
    let mut timestamp: u32 = 0;
    let mut tx_packets: u64 = 0;

    while Instant::now() < deadline {
        ticker.tick().await;

        let rtp = build_conference_rtp(ssrc, seq, timestamp, pkt_size, sender_id);
        seq = seq.wrapping_add(1);
        timestamp = timestamp.wrapping_add(ts_increment);

        if let Some(encrypted) = srtp.encrypt_rtp(&rtp) {
            match socket.send_to(&encrypted, server_addr).await {
                Ok(_) => tx_packets += 1,
                Err(e) => warn!("[{}] send error: {}", id, e),
            }
        }

        // Periodic heartbeat (keep WS alive)
        if tx_packets > 0 && tx_packets % (fps as u64 * 25) == 0 {
            let _ = sig.heartbeat().await;
        }
    }

    sig.close().await;
    info!("[{}] send done: {} pkts", id, tx_packets);
    tx_packets
}

// ═══════════════════════════════════════════════════════════
// Recv loop (per participant — receives from N-1 senders)
// ═══════════════════════════════════════════════════════════

async fn conference_recv_loop(
    id: String,
    socket: Arc<UdpSocket>,
    mut srtp: SrtpCtx,
    deadline: Instant,
) -> RecvResult {
    let mut senders: HashMap<u32, SenderTracker> = HashMap::new();
    let mut buf = vec![0u8; 2048];
    let mut total_recv = 0u64;
    let mut decrypt_fail = 0u64;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() { break; }

        match tokio::time::timeout(remaining, socket.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) => {
                total_recv += 1;

                if !is_rtp(buf[0]) || n < 12 {
                    continue;
                }

                // SRTP decrypt
                let plaintext = match srtp.decrypt_rtp(&buf[..n]) {
                    Some(p) => p,
                    None => {
                        decrypt_fail += 1;
                        if decrypt_fail <= 3 {
                            warn!("[{}] decrypt fail #{} len={}", id, decrypt_fail, n);
                        }
                        continue;
                    }
                };

                // Parse RTP header
                let ssrc = u32::from_be_bytes([
                    plaintext[8], plaintext[9], plaintext[10], plaintext[11],
                ]);
                let seq = u16::from_be_bytes([plaintext[2], plaintext[3]]);

                let tracker = senders.entry(ssrc).or_insert_with(SenderTracker::new);
                tracker.rx_packets += 1;

                // Loss detection per sender SSRC
                if let Some(exp) = tracker.expected_seq {
                    if seq != exp {
                        let gap = seq.wrapping_sub(exp) as u64;
                        if gap > 0 && gap < 1000 {
                            tracker.lost += gap;
                        }
                    }
                }
                tracker.expected_seq = Some(seq.wrapping_add(1));

                // Latency: payload bytes 14-21 = send timestamp (micros LE)
                if plaintext.len() >= 22 {
                    let send_us = u64::from_le_bytes(
                        plaintext[14..22].try_into().unwrap_or([0; 8]),
                    );
                    if send_us > 0 {
                        let recv_us = now_micros();
                        let lat = recv_us.saturating_sub(send_us) as f64;
                        tracker.latencies.push(lat);
                    }
                }
            }
            Ok(Err(e)) => { warn!("[{}] recv error: {}", id, e); }
            Err(_) => break, // timeout = done
        }
    }

    // Aggregate across all senders
    let mut total_rx = 0u64;
    let mut total_lost = 0u64;
    let mut all_latencies: Vec<f64> = Vec::new();

    for tracker in senders.values() {
        total_rx += tracker.rx_packets;
        total_lost += tracker.lost;
        all_latencies.extend(&tracker.latencies);
    }

    let from_count = senders.len() as u32;

    info!("[{}] recv done: rx={} lost={} senders={} total_recv={} decrypt_fail={}",
        id, total_rx, total_lost, from_count, total_recv, decrypt_fail);

    RecvResult { rx_packets: total_rx, rx_lost: total_lost, from_count, latencies: all_latencies }
}

// ═══════════════════════════════════════════════════════════
// Main conference orchestration
// ═══════════════════════════════════════════════════════════

pub async fn run_conference(
    args: &Args,
) -> Result<ConferenceResult, Box<dyn std::error::Error + Send + Sync>> {
    let n = args.participants as usize;
    if n < 2 {
        return Err("conference mode requires --participants >= 2".into());
    }

    info!("[conf] === Conference mode: {} participants, {} streams ===",
        n, n * (n - 1));

    // ───────────────────────────────────────────────────────
    // 1. Setup all participants (sequential — avoids DTLS storm)
    // ───────────────────────────────────────────────────────

    struct SetupParticipant {
        id: String,
        idx: u16,
        sig: SignalingSession,
        pub_socket: Arc<UdpSocket>,
        sub_socket: Arc<UdpSocket>,
        pub_srtp: SrtpCtx,
        sub_srtp: SrtpCtx,
        video_ssrc: u32,
        server_addr: SocketAddr,
    }

    let mut parts: Vec<SetupParticipant> = Vec::new();
    let mut room_id = String::new();

    for i in 0..n {
        let user_id = format!("P{:03}", i + 1);
        let video_ssrc = 90000 + i as u32;

        // Signaling: first creates room, rest join
        let sig = if i == 0 {
            let s = SignalingSession::connect(
                &args.server, args.ws_port, &user_id, &args.room,
            ).await?;
            room_id = s.room_id.clone();
            s
        } else {
            SignalingSession::connect_to_room(
                &args.server, args.ws_port, &user_id, &room_id,
            ).await?
        };

        let sc = sig.server_config.clone().ok_or("no server_config")?;
        let server_addr: SocketAddr = format!("{}:{}", sc.server_ip, sc.server_port).parse()?;

        // Publish PC: STUN → DTLS → SRTP outbound
        let pub_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
        let (client_key, _server_key, client_salt, _server_salt) =
            setup_media_pc(
                &pub_socket, server_addr,
                &sc.pub_ufrag, &sc.pub_pwd,
                &format!("pub:{}", user_id),
            ).await?;

        let mut pub_srtp = SrtpCtx::new();
        pub_srtp.install(&client_key, &client_salt);

        // Subscribe PC: STUN → DTLS → SRTP inbound
        let sub_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
        let (_ck, server_key, _cs, server_salt) =
            setup_media_pc(
                &sub_socket, server_addr,
                &sc.sub_ufrag, &sc.sub_pwd,
                &format!("sub:{}", user_id),
            ).await?;

        let mut sub_srtp = SrtpCtx::new();
        sub_srtp.install(&server_key, &server_salt);

        info!("[conf] {} ready (ssrc={}, pub+sub SRTP installed)", user_id, video_ssrc);

        parts.push(SetupParticipant {
            id: user_id,
            idx: i as u16,
            sig,
            pub_socket,
            sub_socket,
            pub_srtp,
            sub_srtp,
            video_ssrc,
            server_addr,
        });
    }

    // ───────────────────────────────────────────────────────
    // 2. All PUBLISH_TRACKS (register SSRCs with server)
    // ───────────────────────────────────────────────────────
    for p in &mut parts {
        p.sig.publish_tracks(vec![("video".to_string(), p.video_ssrc)]).await?;
    }

    // Wait for TRACKS_UPDATE propagation + PLI
    tokio::time::sleep(Duration::from_millis(500)).await;

    info!("[conf] all {} participants ready — starting benchmark ({}s, {}fps, {}B)",
        n, args.duration, args.fps, args.pkt_size);

    // ───────────────────────────────────────────────────────
    // 3. Spawn send + recv loops for all participants
    // ───────────────────────────────────────────────────────
    let duration = Duration::from_secs(args.duration);
    let deadline = Instant::now() + duration;
    let start = Instant::now();

    let fps = args.fps;
    let pkt_size = args.pkt_size;

    // Collect handles as (send, recv) pairs — same order as parts
    let mut handles: Vec<(
        tokio::task::JoinHandle<u64>,
        tokio::task::JoinHandle<RecvResult>,
    )> = Vec::new();

    for p in parts {
        // recv task (spawn first so it's ready for incoming packets)
        let recv_id = p.id.clone();
        let recv_socket = p.sub_socket;
        let recv_srtp = p.sub_srtp;
        let dl = deadline;
        let recv_handle = tokio::spawn(async move {
            conference_recv_loop(recv_id, recv_socket, recv_srtp, dl).await
        });

        // send task (owns sig for heartbeat)
        let send_id = p.id.clone();
        let send_socket = p.pub_socket;
        let send_srtp = p.pub_srtp;
        let sig = p.sig;
        let ssrc = p.video_ssrc;
        let idx = p.idx;
        let addr = p.server_addr;
        let send_handle = tokio::spawn(async move {
            conference_send_loop(
                send_id, send_socket, send_srtp, sig,
                ssrc, idx, addr, fps, pkt_size, dl,
            ).await
        });

        handles.push((send_handle, recv_handle));
    }

    // ───────────────────────────────────────────────────────
    // 4. Collect results
    // ───────────────────────────────────────────────────────
    let mut part_results: Vec<ParticipantResult> = Vec::new();
    let mut all_latencies: Vec<f64> = Vec::new();
    let mut total_tx: u64 = 0;
    let mut total_rx: u64 = 0;
    let mut total_lost: u64 = 0;
    let mut total_tx_bytes: u64 = 0;
    let mut total_rx_bytes: u64 = 0;

    for (i, (sh, rh)) in handles.into_iter().enumerate() {
        let id = format!("P{:03}", i + 1);

        // Send result
        let tx = match sh.await {
            Ok(tx) => tx,
            Err(e) => { warn!("[conf] send task {} error: {}", id, e); 0 }
        };

        // Recv result
        let rr = match rh.await {
            Ok(rr) => rr,
            Err(e) => {
                warn!("[conf] recv task {} error: {}", id, e);
                RecvResult { rx_packets: 0, rx_lost: 0, from_count: 0, latencies: Vec::new() }
            }
        };

        // Per-participant latency stats
        let mut lats = rr.latencies;
        let p_avg = if lats.is_empty() { 0.0 }
            else { lats.iter().sum::<f64>() / lats.len() as f64 };
        lats.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p_p95 = if lats.is_empty() { 0.0 }
            else { lats[((lats.len() as f64 * 0.95) as usize).min(lats.len() - 1)] };
        let p_max = lats.last().copied().unwrap_or(0.0);

        all_latencies.extend(&lats);
        total_tx += tx;
        total_rx += rr.rx_packets;
        total_lost += rr.rx_lost;
        total_tx_bytes += tx * pkt_size as u64;   // approximate (pre-encrypt size)
        total_rx_bytes += rr.rx_packets * pkt_size as u64;

        part_results.push(ParticipantResult {
            id,
            tx_packets: tx,
            rx_packets: rr.rx_packets,
            rx_lost: rr.rx_lost,
            rx_from_count: rr.from_count,
            latency_avg_us: p_avg,
            latency_p95_us: p_p95,
            latency_max_us: p_max,
        });
    }

    let elapsed = start.elapsed().as_secs_f64();

    // Aggregate latency
    all_latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let latency_avg_us = if all_latencies.is_empty() { 0.0 }
        else { all_latencies.iter().sum::<f64>() / all_latencies.len() as f64 };
    let latency_p95_us = if all_latencies.is_empty() { 0.0 }
        else { all_latencies[((all_latencies.len() as f64 * 0.95) as usize).min(all_latencies.len() - 1)] };
    let latency_max_us = all_latencies.last().copied().unwrap_or(0.0);

    let loss_rate = if total_rx + total_lost == 0 { 0.0 }
        else { total_lost as f64 / (total_rx + total_lost) as f64 * 100.0 };

    let np = n as u32;

    Ok(ConferenceResult {
        duration_secs: elapsed,
        num_participants: np,
        total_streams: np * (np - 1),
        total_tx,
        total_rx,
        total_lost,
        loss_rate,
        input_pps: total_tx as f64 / elapsed,
        output_pps: total_rx as f64 / elapsed,
        input_mbps: (total_tx_bytes as f64 * 8.0) / (elapsed * 1_000_000.0),
        output_mbps: (total_rx_bytes as f64 * 8.0) / (elapsed * 1_000_000.0),
        latency_avg_us,
        latency_p95_us,
        latency_max_us,
        participants: part_results,
    })
}
