// author: kodeholic (powered by Claude)
//! Media transport — publisher + subscriber benchmark
//!
//! B-1: STUN + DTLS + SRTP setup ✅
//! B-2: subscriber auto setup (subscribe PC: STUN + DTLS)
//! B-3: subscriber RTP recv + latency/loss measurement
//! B-4: aggregate report

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{Instant, interval};
use tracing::{error, info, warn};

use crate::signaling::SignalingSession;
use crate::stun;
use crate::Args;

// ── DemuxConn (same pattern as server) ──

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex;
use webrtc_util::conn::Conn;

pub type DtlsPacketTx = mpsc::Sender<Bytes>;

struct DemuxConn {
    socket: Arc<UdpSocket>,
    peer_addr: SocketAddr,
    rx: Mutex<mpsc::Receiver<Bytes>>,
}

impl DemuxConn {
    fn new(socket: Arc<UdpSocket>, peer_addr: SocketAddr) -> (Self, DtlsPacketTx) {
        let (tx, rx) = mpsc::channel(128);
        (
            Self {
                socket,
                peer_addr,
                rx: Mutex::new(rx),
            },
            tx,
        )
    }
}

#[async_trait]
impl Conn for DemuxConn {
    async fn connect(&self, _addr: SocketAddr) -> webrtc_util::Result<()> { Ok(()) }
    async fn recv(&self, buf: &mut [u8]) -> webrtc_util::Result<usize> {
        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(data) => {
                let len = data.len().min(buf.len());
                buf[..len].copy_from_slice(&data[..len]);
                Ok(len)
            }
            None => Err(webrtc_util::Error::Other("channel closed".into())),
        }
    }
    async fn recv_from(&self, buf: &mut [u8]) -> webrtc_util::Result<(usize, SocketAddr)> {
        let n = self.recv(buf).await?;
        Ok((n, self.peer_addr))
    }
    async fn send(&self, buf: &[u8]) -> webrtc_util::Result<usize> {
        self.socket
            .send_to(buf, self.peer_addr)
            .await
            .map_err(|e| webrtc_util::Error::Other(e.to_string()))
    }
    async fn send_to(&self, buf: &[u8], _target: SocketAddr) -> webrtc_util::Result<usize> {
        self.send(buf).await
    }
    fn local_addr(&self) -> webrtc_util::Result<SocketAddr> {
        self.socket.local_addr().map_err(|e| webrtc_util::Error::Other(e.to_string()))
    }
    fn remote_addr(&self) -> Option<SocketAddr> { Some(self.peer_addr) }
    async fn close(&self) -> webrtc_util::Result<()> { Ok(()) }
    fn as_any(&self) -> &(dyn std::any::Any + Send + Sync) { self }
}

// ── SRTP wrapper ──

pub struct SrtpCtx {
    inner: Option<webrtc_srtp::context::Context>,
}

impl SrtpCtx {
    pub fn new() -> Self { Self { inner: None } }
    pub fn install(&mut self, key: &[u8], salt: &[u8]) {
        match webrtc_srtp::context::Context::new(
            key, salt,
            webrtc_srtp::protection_profile::ProtectionProfile::Aes128CmHmacSha1_80,
            None, None,
        ) {
            Ok(ctx) => self.inner = Some(ctx),
            Err(e) => error!("SRTP install failed: {:?}", e),
        }
    }
    pub fn encrypt_rtp(&mut self, pkt: &[u8]) -> Option<Vec<u8>> {
        self.inner.as_mut()?.encrypt_rtp(pkt).ok().map(|b: bytes::Bytes| b.to_vec())
    }
    pub fn decrypt_rtp(&mut self, pkt: &[u8]) -> Option<Vec<u8>> {
        self.inner.as_mut()?.decrypt_rtp(pkt).ok().map(|b: bytes::Bytes| b.to_vec())
    }
}

// ── Demux ──

fn is_stun(b: u8) -> bool { b <= 0x03 }
fn is_dtls(b: u8) -> bool { (0x14..=0x3F).contains(&b) }
pub fn is_rtp(b: u8) -> bool { (0x80..=0xBF).contains(&b) }

// ── Benchmark result ──

#[derive(Debug, Default)]
pub struct BenchResult {
    // publisher
    pub duration_secs: f64,
    pub tx_packets: u64,
    pub tx_bytes: u64,
    pub tx_pps: f64,
    pub tx_mbps: f64,
    // subscriber aggregate
    pub num_subscribers: u32,
    pub fan_out: u32,
    pub rx_total_packets: u64,
    pub rx_total_bytes: u64,
    pub rx_total_lost: u64,
    pub rx_total_pps: f64,
    pub rx_total_mbps: f64,
    pub loss_rate: f64,
    // latency (microseconds)
    pub latency_avg_us: f64,
    pub latency_p95_us: f64,
    pub latency_max_us: f64,
    // per-subscriber detail
    pub sub_details: Vec<SubDetail>,
}

#[derive(Debug, Default, Clone)]
pub struct SubDetail {
    pub id: String,
    pub rx_packets: u64,
    pub rx_bytes: u64,
    pub rx_lost: u64,
    pub latency_avg_us: f64,
    pub latency_p95_us: f64,
    pub latency_max_us: f64,
}

// ── Time helpers ──

pub fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

// ── STUN + DTLS setup (reusable for both publish/subscribe PC) ──

pub async fn setup_media_pc(
    socket: &Arc<UdpSocket>,
    server_addr: SocketAddr,
    ufrag: &str,
    pwd: &str,
    label: &str,
) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
    // 1. STUN binding request
    let client_ufrag = random_ice_string(4);
    let stun_req = stun::build_binding_request(ufrag, &client_ufrag, pwd);
    socket.send_to(&stun_req, server_addr).await?;
    info!("[{}] STUN binding request sent ufrag={}", label, ufrag);

    // Wait STUN response
    let mut buf = vec![0u8; 2048];
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut stun_ok = false;

    while Instant::now() < deadline {
        let timeout = deadline - Instant::now();
        match tokio::time::timeout(timeout, socket.recv_from(&mut buf)).await {
            Ok(Ok((n, _))) => {
                if is_stun(buf[0]) && n >= 20 {
                    let msg_type = u16::from_be_bytes([buf[0], buf[1]]);
                    if msg_type == 0x0101 {
                        info!("[{}] STUN response received", label);
                        stun_ok = true;
                        break;
                    }
                }
            }
            Ok(Err(e)) => warn!("[{}] recv error: {}", label, e),
            Err(_) => break,
        }
    }
    if !stun_ok {
        return Err(format!("{} STUN timeout", label).into());
    }

    // 2. DTLS active handshake
    let (demux_conn, dtls_tx) = DemuxConn::new(Arc::clone(socket), server_addr);
    let demux_conn = Arc::new(demux_conn);

    // Recv loop for DTLS packets
    let socket_recv = Arc::clone(socket);
    let dtls_tx_clone = dtls_tx.clone();
    let recv_handle = tokio::spawn(async move {
        let mut b = vec![0u8; 2048];
        loop {
            match socket_recv.recv_from(&mut b).await {
                Ok((n, _)) => {
                    if is_dtls(b[0]) {
                        let _ = dtls_tx_clone.send(Bytes::copy_from_slice(&b[..n])).await;
                    }
                }
                Err(_) => break,
            }
        }
    });

    info!("[{}] DTLS active handshake starting", label);
    let dtls_config = dtls::config::Config {
        insecure_skip_verify: true,
        srtp_protection_profiles: vec![
            dtls::extension::extension_use_srtp::SrtpProtectionProfile::Srtp_Aes128_Cm_Hmac_Sha1_80,
        ],
        extended_master_secret: dtls::config::ExtendedMasterSecretType::Require,
        ..Default::default()
    };

    let dtls_conn = dtls::conn::DTLSConn::new(
        demux_conn as Arc<dyn Conn + Send + Sync>,
        dtls_config,
        true,
        None,
    )
    .await
    .map_err(|e| format!("{} DTLS failed: {:?}", label, e))?;

    info!("[{}] DTLS handshake completed", label);

    // 3. Export SRTP keys
    let state = dtls_conn.connection_state().await;
    use webrtc_util::KeyingMaterialExporter;
    let mat = state
        .export_keying_material("EXTRACTOR-dtls_srtp", &[], 60)
        .await
        .map_err(|e| format!("export_keying_material: {:?}", e))?;

    recv_handle.abort();

    // RFC 5764: client_key(16) + server_key(16) + client_salt(14) + server_salt(14)
    let client_key  = mat[0..16].to_vec();
    let server_key  = mat[16..32].to_vec();
    let client_salt = mat[32..46].to_vec();
    let server_salt = mat[46..60].to_vec();

    Ok((client_key, server_key, client_salt, server_salt))
}

// ── Main benchmark ──

pub async fn run_benchmark(
    args: &Args,
) -> Result<BenchResult, Box<dyn std::error::Error + Send + Sync>> {
    // ═══════════════════════════════════════════════════════════
    // 1. Publisher setup
    // ═══════════════════════════════════════════════════════════
    let pub_id = "B001".to_string();
    let mut pub_sig = SignalingSession::connect(
        &args.server, args.ws_port, &pub_id, &args.room,
    ).await?;

    let pub_sc = pub_sig.server_config.clone().ok_or("no server_config")?;
    let room_id = pub_sig.room_id.clone();
    let server_addr: SocketAddr = format!("{}:{}", pub_sc.server_ip, pub_sc.server_port).parse()?;

    let pub_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    info!("[pub] socket={} server={}", pub_socket.local_addr()?, server_addr);

    let (client_key, _server_key, client_salt, _server_salt) =
        setup_media_pc(&pub_socket, server_addr, &pub_sc.pub_ufrag, &pub_sc.pub_pwd, "pub").await?;

    let mut pub_srtp = SrtpCtx::new();
    pub_srtp.install(&client_key, &client_salt); // outbound: client→server
    info!("[pub] SRTP outbound ready");

    // ═══════════════════════════════════════════════════════════
    // 2. Subscriber setup (sequential for simplicity)
    // ═══════════════════════════════════════════════════════════
    struct SubSession {
        id: String,
        socket: Arc<UdpSocket>,
        srtp: SrtpCtx,
        sig: SignalingSession,
    }

    let mut subs: Vec<SubSession> = Vec::new();

    for i in 0..args.subscribers {
        let sub_id = format!("S{:03}", i + 1);
        let sig = SignalingSession::connect_to_room(
            &args.server, args.ws_port, &sub_id, &room_id,
        ).await?;

        let sc = sig.server_config.clone().ok_or("no server_config")?;
        let socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
        let label = format!("sub:{}", sub_id);

        let (_ck, server_key, _cs, server_salt) =
            setup_media_pc(&socket, server_addr, &sc.sub_ufrag, &sc.sub_pwd, &label).await?;

        let mut srtp = SrtpCtx::new();
        srtp.install(&server_key, &server_salt); // inbound: server→client
        info!("[{}] SRTP inbound ready", sub_id);

        subs.push(SubSession { id: sub_id, socket, srtp, sig });
    }

    // ═══════════════════════════════════════════════════════════
    // 3. PUBLISH_TRACKS (after subscribers are ready)
    // ═══════════════════════════════════════════════════════════
    let video_ssrc: u32 = 90000;
    pub_sig.publish_tracks(vec![("video".to_string(), video_ssrc)]).await?;

    // Small delay for TRACKS_UPDATE propagation + PLI
    tokio::time::sleep(Duration::from_millis(200)).await;

    // ═══════════════════════════════════════════════════════════
    // 4. Spawn subscriber recv loops
    // ═══════════════════════════════════════════════════════════
    let duration = Duration::from_secs(args.duration);
    let deadline = Instant::now() + duration;

    // Channel per subscriber for results
    let mut sub_handles = Vec::new();

    for sub in subs {
        let dl = deadline;
        let handle = tokio::spawn(async move {
            subscriber_recv_loop(sub.id, sub.socket, sub.srtp, dl, sub.sig).await
        });
        sub_handles.push(handle);
    }

    // ═══════════════════════════════════════════════════════════
    // 5. Publisher send loop
    // ═══════════════════════════════════════════════════════════
    info!("[bench] starting: {}fps × {}B for {}s, {} subscribers",
        args.fps, args.pkt_size, args.duration, args.subscribers);

    let frame_interval = Duration::from_micros(1_000_000 / args.fps as u64);
    let mut ticker = interval(frame_interval);
    let ts_increment = 90000 / args.fps;

    let mut seq: u16 = 0;
    let mut timestamp: u32 = 0;
    let mut tx_packets: u64 = 0;
    let mut tx_bytes: u64 = 0;

    let start = Instant::now();

    while Instant::now() < deadline {
        ticker.tick().await;

        let rtp = build_fake_rtp(video_ssrc, seq, timestamp, args.pkt_size);
        seq = seq.wrapping_add(1);
        timestamp = timestamp.wrapping_add(ts_increment);

        if let Some(encrypted) = pub_srtp.encrypt_rtp(&rtp) {
            match pub_socket.send_to(&encrypted, server_addr).await {
                Ok(n) => {
                    tx_packets += 1;
                    tx_bytes += n as u64;
                }
                Err(e) => warn!("[pub] send error: {}", e),
            }
        }

        // Periodic heartbeat
        if tx_packets > 0 && tx_packets % (args.fps as u64 * 25) == 0 {
            let _ = pub_sig.heartbeat().await;
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    info!("[pub] done: {} pkts in {:.1}s", tx_packets, elapsed);

    // ═══════════════════════════════════════════════════════════
    // 6. Collect subscriber results
    // ═══════════════════════════════════════════════════════════
    let mut all_latencies: Vec<f64> = Vec::new();
    let mut sub_details: Vec<SubDetail> = Vec::new();
    let mut rx_total_packets: u64 = 0;
    let mut rx_total_bytes: u64 = 0;
    let mut rx_total_lost: u64 = 0;

    for handle in sub_handles {
        match handle.await {
            Ok(detail) => {
                rx_total_packets += detail.rx_packets;
                rx_total_bytes += detail.rx_bytes;
                rx_total_lost += detail.rx_lost;
                sub_details.push(detail);
            }
            Err(e) => warn!("[bench] subscriber task error: {}", e),
        }
    }

    // Aggregate latency from all subscriber details
    // (p95/max already computed per subscriber)
    for d in &sub_details {
        // Use avg as representative sample per subscriber
        if d.latency_avg_us > 0.0 {
            all_latencies.push(d.latency_avg_us);
        }
    }

    let latency_avg_us = if all_latencies.is_empty() { 0.0 }
        else { all_latencies.iter().sum::<f64>() / all_latencies.len() as f64 };
    let latency_p95_us = sub_details.iter()
        .map(|d| d.latency_p95_us)
        .fold(0.0f64, f64::max);
    let latency_max_us = sub_details.iter()
        .map(|d| d.latency_max_us)
        .fold(0.0f64, f64::max);

    let loss_rate = if rx_total_packets + rx_total_lost == 0 { 0.0 }
        else { rx_total_lost as f64 / (rx_total_packets + rx_total_lost) as f64 * 100.0 };

    // Cleanup
    pub_sig.close().await;

    Ok(BenchResult {
        duration_secs: elapsed,
        tx_packets,
        tx_bytes,
        tx_pps: tx_packets as f64 / elapsed,
        tx_mbps: (tx_bytes as f64 * 8.0) / (elapsed * 1_000_000.0),
        num_subscribers: args.subscribers,
        fan_out: args.subscribers,
        rx_total_packets,
        rx_total_bytes,
        rx_total_lost,
        rx_total_pps: rx_total_packets as f64 / elapsed,
        rx_total_mbps: (rx_total_bytes as f64 * 8.0) / (elapsed * 1_000_000.0),
        loss_rate,
        latency_avg_us,
        latency_p95_us,
        latency_max_us,
        sub_details,
    })
}

// ═══════════════════════════════════════════════════════════
// Subscriber recv loop
// ═══════════════════════════════════════════════════════════

async fn subscriber_recv_loop(
    id: String,
    socket: Arc<UdpSocket>,
    mut srtp: SrtpCtx,
    deadline: Instant,
    _sig: SignalingSession, // keep WS alive during benchmark
) -> SubDetail {
    let mut rx_packets: u64 = 0;
    let mut rx_bytes: u64 = 0;
    let mut latencies: Vec<f64> = Vec::new();
    let mut expected_seq: Option<u16> = None;
    let mut lost: u64 = 0;

    let mut buf = vec![0u8; 2048];
    let mut total_recv = 0u64;
    let mut non_rtp = 0u64;
    let mut decrypt_fail = 0u64;

    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() { break; }

        match tokio::time::timeout(remaining, socket.recv_from(&mut buf)).await {
            Ok(Ok((n, from))) => {
                total_recv += 1;
                if total_recv <= 3 {
                    info!("[{}] recv #{} {}B from {} byte0=0x{:02X}",
                        id, total_recv, n, from, buf[0]);
                }

                if !is_rtp(buf[0]) || n < 12 {
                    non_rtp += 1;
                    continue;
                }

                // SRTP decrypt
                let plaintext = match srtp.decrypt_rtp(&buf[..n]) {
                    Some(p) => p,
                    None => {
                        decrypt_fail += 1;
                        if decrypt_fail <= 3 {
                            warn!("[{}] SRTP decrypt FAILED #{} len={}", id, decrypt_fail, n);
                        }
                        continue;
                    }
                };

                rx_packets += 1;
                rx_bytes += n as u64;

                // Parse RTP header
                let seq = u16::from_be_bytes([plaintext[2], plaintext[3]]);

                // Loss detection
                if let Some(exp) = expected_seq {
                    if seq != exp {
                        let gap = seq.wrapping_sub(exp) as u64;
                        if gap > 0 && gap < 1000 { // reasonable gap
                            lost += gap;
                        }
                    }
                }
                expected_seq = Some(seq.wrapping_add(1));

                // Latency: first 8 bytes of payload = send timestamp (micros)
                if plaintext.len() >= 20 { // 12 header + 8 timestamp
                    let send_us = u64::from_le_bytes(
                        plaintext[12..20].try_into().unwrap_or([0; 8])
                    );
                    if send_us > 0 {
                        let recv_us = now_micros();
                        let lat = recv_us.saturating_sub(send_us) as f64;
                        latencies.push(lat);
                    }
                }
            }
            Ok(Err(e)) => { warn!("[{}] recv error: {}", id, e); }
            Err(_) => break, // timeout = done
        }
    }

    // Compute stats
    let latency_avg_us = if latencies.is_empty() { 0.0 }
        else { latencies.iter().sum::<f64>() / latencies.len() as f64 };

    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let latency_p95_us = if latencies.is_empty() { 0.0 }
        else { latencies[((latencies.len() as f64 * 0.95) as usize).min(latencies.len() - 1)] };
    let latency_max_us = latencies.last().copied().unwrap_or(0.0);

    info!("[{}] done: rx={} lost={} total_recv={} non_rtp={} decrypt_fail={} avg={:.0}us p95={:.0}us max={:.0}us",
        id, rx_packets, lost, total_recv, non_rtp, decrypt_fail, latency_avg_us, latency_p95_us, latency_max_us);

    SubDetail {
        id,
        rx_packets,
        rx_bytes,
        rx_lost: lost,
        latency_avg_us,
        latency_p95_us,
        latency_max_us,
    }
}

// ═══════════════════════════════════════════════════════════
// Fake RTP builder (with embedded send timestamp)
// ═══════════════════════════════════════════════════════════

fn build_fake_rtp(ssrc: u32, seq: u16, timestamp: u32, total_size: usize) -> Vec<u8> {
    let payload_size = if total_size > 12 { total_size - 12 } else { 0 };
    let mut pkt = Vec::with_capacity(total_size);

    // RTP header (12 bytes)
    pkt.push(0x80); // V=2
    pkt.push(96);   // PT=96 (VP8)
    pkt.extend_from_slice(&seq.to_be_bytes());
    pkt.extend_from_slice(&timestamp.to_be_bytes());
    pkt.extend_from_slice(&ssrc.to_be_bytes());

    // Payload: first 8 bytes = send timestamp (micros LE)
    if payload_size >= 8 {
        let ts = now_micros();
        pkt.extend_from_slice(&ts.to_le_bytes());
        pkt.resize(12 + payload_size, 0x00);
    } else {
        pkt.resize(12 + payload_size, 0x00);
    }

    pkt
}

fn random_ice_string(len: usize) -> String {
    let mut bytes = vec![0u8; len];
    getrandom::fill(&mut bytes).expect("getrandom failed");
    bytes.iter().map(|b| {
        let r = b % 36;
        if r < 10 { (b'0' + r) as char } else { (b'a' + r - 10) as char }
    }).collect()
}
