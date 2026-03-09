// author: kodeholic (powered by Claude)
//! Media transport — STUN + DTLS + SRTP setup (shared)
//!
//! Provides: DemuxConn, SrtpCtx, setup_media_pc, demux helpers, time helpers

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{error, info, warn};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::Mutex;
use webrtc_util::conn::Conn;

use crate::stun;

// ── DemuxConn (DTLS 패킷만 받는 가상 커넥션) ──

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

    pub fn encrypt_rtcp(&mut self, pkt: &[u8]) -> Option<Vec<u8>> {
        self.inner.as_mut()?.encrypt_rtcp(pkt).ok().map(|b: bytes::Bytes| b.to_vec())
    }

    pub fn decrypt_rtcp(&mut self, pkt: &[u8]) -> Option<Vec<u8>> {
        self.inner.as_mut()?.decrypt_rtcp(pkt).ok().map(|b: bytes::Bytes| b.to_vec())
    }
}

// ── Demux helpers ──

pub fn is_stun(b: u8) -> bool { b <= 0x03 }
pub fn is_dtls(b: u8) -> bool { (0x14..=0x3F).contains(&b) }
pub fn is_rtp(b: u8) -> bool { (0x80..=0xBF).contains(&b) }

// ── Time helpers ──

pub fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

// ── ICE string generator ──

pub fn random_ice_string(len: usize) -> String {
    let mut bytes = vec![0u8; len];
    getrandom::fill(&mut bytes).expect("getrandom failed");
    bytes.iter().map(|b| {
        let r = b % 36;
        if r < 10 { (b'0' + r) as char } else { (b'a' + r - 10) as char }
    }).collect()
}

// ── STUN + DTLS setup (reusable for both publish/subscribe PC) ──

/// Perform STUN binding + DTLS handshake + export SRTP keying material.
///
/// Returns (client_key, server_key, client_salt, server_salt)
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
