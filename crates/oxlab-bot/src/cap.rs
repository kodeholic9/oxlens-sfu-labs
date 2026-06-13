// author: kodeholic (powered by Claude)
//! Capacity 측정 봇 — N 스윕 부하 천장 측정용 경량 봇.
//!
//! 설계: `context/design/20260613_capacity_test_design.md` §5 (복호-skip), §8 (병목 분리).
//!
//! 기존 oxlab-bot(L1 관측, 4 fixed task)과 달리 capacity 봇은 측정에 필요한 것만:
//! - publisher: PUBLISH_TRACKS + RTP 송신(send_attempt/ok 카운터) + WS 펌프.
//! - subscriber: RTP 수신 — **Count**(헤더 12B 평문 파싱, 복호 skip) / **Full**(전수 복호 + latency).
//!   + WS 펌프(TRACKS_UPDATE → tracks_ready → SubscriberGate resume).
//!
//! 복호-skip 핵심(§5): SRTP 헤더 12B 는 평문 — ssrc/seq 무복호 파싱으로 loss(seq gap)·도착
//! 카운트. SFU egress 복제 부하는 그대로, 봇 CPU 천장은 위로. **측정 도구가 측정 대상보다 가볍다.**

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::net::UdpSocket;
use tokio::sync::{watch, Mutex, Semaphore};
use tokio::time::{Instant, interval, sleep_until, timeout};
use tracing::{debug, warn};

use oxlens_lab_common::media::{self, SrtpCtx, is_rtp, now_micros};
use oxlens_lab_common::signaling::{SignalingSession, OP_TRACKS_UPDATE};

use crate::rtcp_parser::simple_hash;
use crate::rtp_publisher::{build_publish_intent_json, RtpPublisherConfig, RtpPublisherState};

type DynErr = Box<dyn std::error::Error + Send + Sync>;

/// 수신 모드 — 복호-skip 핵심 트릭.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum RecvMode {
    /// 헤더 12B 평문만 파싱 (복호 안 함). loss/카운트만. capacity sub 기본.
    Count,
    /// 전수 복호 → latency(send ts) 측정. 대표 봇 2~3개만.
    Full,
}

/// capacity 봇 누적 카운터 (Arc 공유, lock-free atomic).
#[derive(Default)]
pub struct CapCounters {
    /// 송신 시도 (publisher).
    pub tx_attempt: AtomicU64,
    /// 송신 성공 (publisher) — tx_attempt 대비 부족 = 봇 송신 천장(§8 bot_healthy).
    pub tx_ok: AtomicU64,
    /// 송신 바이트 (publisher, SRTP encrypt 후).
    pub tx_bytes: AtomicU64,
    /// 수신 RTP 패킷 (subscriber).
    pub rx_packets: AtomicU64,
    /// 수신 바이트 (subscriber, wire).
    pub rx_bytes: AtomicU64,
    /// seq-gap loss (subscriber).
    pub rx_lost: AtomicU64,
    /// 복호 실패 (Full subscriber) — SRTP 키 불일치/순서.
    pub rx_decrypt_fail: AtomicU64,
    /// 실제 fan-out RTP 를 1패킷이라도 받은 sub 수 (곡선 신뢰성 — active/N).
    pub active_subs: AtomicUsize,
}

impl CapCounters {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

/// 봇별 RTP config (user_id 해시로 ssrc 하위 24비트 — oxe2e/labs 정합).
fn rtp_config_for(id: &str) -> RtpPublisherConfig {
    let h = simple_hash(id);
    RtpPublisherConfig {
        audio_ssrc: 0xA000_0000 | (h & 0x00FF_FFFF),
        video_ssrc: 0xB000_0000 | (h & 0x00FF_FFFF),
        ..RtpPublisherConfig::default()
    }
}

/// video RTP 평문 끝 8B 에 send ts(µs) 심기 — Full sub latency 측정용.
/// payload(VP8 fake 200/800B)에 여유 있음. SRTP encrypt 전 평문에 심는다.
fn embed_send_ts(pkt: &mut [u8]) {
    let n = pkt.len();
    if n >= 8 {
        pkt[n - 8..].copy_from_slice(&now_micros().to_le_bytes());
    }
}

/// SRTP encrypt + UDP send + 카운터. 송신 천장 관측(tx_attempt vs tx_ok).
async fn send_rtp(
    srtp: &mut SrtpCtx,
    socket: &UdpSocket,
    addr: SocketAddr,
    pkt: &[u8],
    counters: &CapCounters,
) {
    counters.tx_attempt.fetch_add(1, Ordering::Relaxed);
    if let Some(enc) = srtp.encrypt_rtp(pkt) {
        if socket.send_to(&enc, addr).await.is_ok() {
            counters.tx_ok.fetch_add(1, Ordering::Relaxed);
            counters.tx_bytes.fetch_add(enc.len() as u64, Ordering::Relaxed);
        }
    }
}

/// STUN consent 1회 — peer.last_seen 갱신(미디어 PC liveness, reaper 회피).
async fn send_consent(socket: &UdpSocket, addr: SocketAddr, ufrag: &str, pwd: &str) {
    let req = oxlens_lab_common::stun::build_binding_request(
        ufrag, &media::random_ice_string(4), pwd,
    );
    let _ = socket.send_to(&req, addr).await;
}

/// Conference 봇 — N명 전원 pub+sub (raw mesh). 각 봇이 발행+수신 동시.
/// simulcast off (raw mesh = N×(N-1) 스트림 — 소규모 현실값만, 설계 §6).
#[allow(clippy::too_many_arguments)]
pub async fn run_conf_bot(
    id: String,
    server: String,
    ws_port: u16,
    room_id: String,
    is_creator: bool,
    duration: Duration,
    recv_mode: RecvMode,
    counters: Arc<CapCounters>,
    latencies: Arc<Mutex<Vec<f64>>>,
    setup_sem: Arc<Semaphore>,
    ready: Arc<AtomicUsize>,
    mut trigger: watch::Receiver<bool>,
) -> Result<(), DynErr> {
    // ── setup: pub PC + sub PC 둘 다 (sem 제한 + timeout) ──
    let (mut session, pub_socket, mut pub_srtp, sub_socket, mut sub_srtp, server_addr, pub_uf, pub_pw, sub_uf, sub_pw) = {
        let _permit = setup_sem.acquire().await.map_err(|e| format!("sem: {e}"))?;
        let id_s = id.clone();
        let room_s = room_id.clone();
        let srv = server.clone();
        let fut = async move {
            let session = if is_creator {
                SignalingSession::connect_with_room_id(&srv, ws_port, &id_s, &room_s).await?
            } else {
                SignalingSession::connect_to_room(&srv, ws_port, &id_s, &room_s).await?
            };
            let sc = session.server_config.clone().ok_or("no server_config")?;
            let server_addr: SocketAddr = format!("{}:{}", sc.server_ip, sc.server_port).parse()?;
            let pub_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
            let (ck, _sk, cs, _ss) = media::setup_media_pc(
                &pub_socket, server_addr, &sc.pub_ufrag, &sc.pub_pwd, &format!("pub:{id_s}"),
            ).await?;
            let mut pub_srtp = SrtpCtx::new();
            pub_srtp.install(&ck, &cs);
            let sub_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
            let (_ck2, sk, _cs2, ss) = media::setup_media_pc(
                &sub_socket, server_addr, &sc.sub_ufrag, &sc.sub_pwd, &format!("sub:{id_s}"),
            ).await?;
            let mut sub_srtp = SrtpCtx::new();
            sub_srtp.install(&sk, &ss);
            Ok::<_, DynErr>((
                session, pub_socket, pub_srtp, sub_socket, sub_srtp, server_addr,
                sc.pub_ufrag.clone(), sc.pub_pwd.clone(), sc.sub_ufrag.clone(), sc.sub_pwd.clone(),
            ))
        };
        match timeout(Duration::from_secs(20), fut).await {
            Ok(Ok(t)) => t,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(format!("[cap-conf:{id}] setup timeout").into()),
        }
    };
    ready.fetch_add(1, Ordering::Relaxed);

    // ── 발행 트리거 대기 (전원 setup 완료 후 동시 발행). consent 로 liveness 유지 ──
    {
        let mut pump = interval(Duration::from_millis(100));
        let mut consent = interval(Duration::from_secs(3));
        loop {
            tokio::select! {
                r = trigger.changed() => { if r.is_err() || *trigger.borrow() { break; } }
                _ = consent.tick() => {
                    send_consent(&pub_socket, server_addr, &pub_uf, &pub_pw).await;
                    send_consent(&sub_socket, server_addr, &sub_uf, &sub_pw).await;
                }
                _ = pump.tick() => { session.poll_events().await; }
            }
        }
    }

    // ── 발행 (full-duplex, simulcast off) ──
    let rcfg = rtp_config_for(&id);
    session.request_routed(
        oxlens_lab_common::signaling::OP_PUBLISH_TRACKS,
        build_publish_intent_json(&rcfg, "full"),
    ).await?;

    // ── measure: RTP 송신(pub) + 수신(sub) + consent(sub) + pump ──
    let mut a_state = RtpPublisherState::new(rcfg.clone());
    let mut v_state = RtpPublisherState::new(rcfg.clone());
    let mut a_tick = interval(Duration::from_millis(20));
    let mut v_tick = interval(Duration::from_millis(33));
    let mut pump = interval(Duration::from_millis(100));
    let mut consent = interval(Duration::from_secs(3));
    let mut buf = vec![0u8; 2048];
    let mut last_seq: HashMap<u32, u16> = HashMap::new();
    let mut local_lat: Vec<f64> = Vec::new();
    let mut seen_rtp = false;
    let deadline = Instant::now() + duration;

    loop {
        tokio::select! {
            _ = sleep_until(deadline) => break,
            _ = a_tick.tick() => {
                let pkt = a_state.build_opus_packet();
                send_rtp(&mut pub_srtp, &pub_socket, server_addr, &pkt, &counters).await;
            }
            _ = v_tick.tick() => {
                let mut pkt = v_state.build_vp8_packet();
                embed_send_ts(&mut pkt);
                send_rtp(&mut pub_srtp, &pub_socket, server_addr, &pkt, &counters).await;
            }
            r = sub_socket.recv_from(&mut buf) => {
                if let Ok((nb, _)) = r {
                    let got = handle_rtp(&buf[..nb], recv_mode, &mut sub_srtp,
                        &mut last_seq, &counters, &mut local_lat);
                    if got && !seen_rtp {
                        seen_rtp = true;
                        counters.active_subs.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            _ = consent.tick() => {
                send_consent(&sub_socket, server_addr, &sub_uf, &sub_pw).await;
            }
            _ = pump.tick() => {
                session.poll_events().await;
                if !session.drain_all_events(OP_TRACKS_UPDATE).is_empty() {
                    let _ = session.tracks_ready().await;
                }
            }
        }
    }
    session.close().await;
    if !local_lat.is_empty() {
        latencies.lock().await.extend(local_lat);
    }
    Ok(())
}

/// Publisher 봇 — 방 ensure(create) + 발행 + RTP 송신. duration 동안 구동.
pub async fn run_publisher(
    id: String,
    server: String,
    ws_port: u16,
    room_id: String,
    duplex: &'static str,
    duration: Duration,
    counters: Arc<CapCounters>,
    mut trigger: watch::Receiver<bool>,
) -> Result<(), DynErr> {
    let mut session =
        SignalingSession::connect_with_room_id(&server, ws_port, &id, &room_id).await?;
    let sc = session.server_config.clone().ok_or("no server_config")?;
    let server_addr: SocketAddr = format!("{}:{}", sc.server_ip, sc.server_port).parse()?;

    // publish PC (STUN/DTLS/SRTP)
    let pub_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    let (ck, _sk, cs, _ss) = media::setup_media_pc(
        &pub_socket, server_addr, &sc.pub_ufrag, &sc.pub_pwd, &format!("pub:{id}"),
    )
    .await?;
    let mut srtp = SrtpCtx::new();
    srtp.install(&ck, &cs);

    let rcfg = rtp_config_for(&id);

    // 발행 트리거 대기 — runner 가 전 sub setup 완료 후 신호(notify_new_stream 이 전 sub 도달).
    // 그 사이 WS 펌프(ACK) + STUN consent(미디어 PC liveness — reaper 회피, RTP 전 idle).
    {
        let mut pump = interval(Duration::from_millis(100));
        let mut consent = interval(Duration::from_secs(3));
        loop {
            tokio::select! {
                r = trigger.changed() => {
                    if r.is_err() || *trigger.borrow() { break; }
                }
                _ = consent.tick() => {
                    let req = oxlens_lab_common::stun::build_binding_request(
                        &sc.pub_ufrag, &media::random_ice_string(4), &sc.pub_pwd,
                    );
                    let _ = pub_socket.send_to(&req, server_addr).await;
                }
                _ = pump.tick() => {
                    session.poll_events().await;
                }
            }
        }
    }

    let payload = build_publish_intent_json(&rcfg, duplex);
    session.request_routed(
        oxlens_lab_common::signaling::OP_PUBLISH_TRACKS, payload,
    ).await?;
    debug!("[cap-pub:{id}] PUBLISH_TRACKS sent (duplex={duplex})");

    // RTP 송신 + WS 펌프 단일 루프.
    let mut a_state = RtpPublisherState::new(rcfg.clone());
    let mut v_state = RtpPublisherState::new(rcfg.clone());
    let mut a_tick = interval(Duration::from_millis(20));
    let mut v_tick = interval(Duration::from_millis(33));
    let mut pump = interval(Duration::from_millis(100));
    // ptt(half): floor 주기 재전송. RTP 가 트랙 등록(has_half_duplex 가드 통과)한 뒤 grant 되도록
    // 1초 주기 FREQ — 단일 speaker = 멱등(이미 speaker 면 무시). bearer=dc 여도 grant→fan-out 됨.
    let mut floor_tick = interval(Duration::from_secs(1));
    let is_ptt = duplex == "half";
    let deadline = Instant::now() + duration;

    loop {
        tokio::select! {
            _ = sleep_until(deadline) => break,
            _ = a_tick.tick() => {
                let pkt = a_state.build_opus_packet();
                send_rtp(&mut srtp, &pub_socket, server_addr, &pkt, &counters).await;
            }
            _ = v_tick.tick() => {
                let mut pkt = v_state.build_vp8_packet();
                embed_send_ts(&mut pkt);
                send_rtp(&mut srtp, &pub_socket, server_addr, &pkt, &counters).await;
            }
            _ = floor_tick.tick(), if is_ptt => {
                // FLOOR_MBCP FREQ (WS binary fallback, §4) — native TLV(TS 24.380, 서버 mbcp_native).
                // 트랙 등록(has_half_duplex 가드) 후 grant → slot.set_publisher → fan-out.
                let freq = oxlens_lab_common::mbcp::build_native_freq(10, &room_id);
                if let Err(e) = session.send_floor_mbcp(&freq).await {
                    warn!("[cap-pub:{id}] FREQ send failed: {e}");
                }
            }
            _ = pump.tick() => {
                session.poll_events().await; // 이벤트 ACK 자동 (연결 유지)
                for raw in session.drain_floor_mbcp() {
                    let ty = raw.first().copied().unwrap_or(0) & 0x0F;
                    if ty == 2 {
                        debug!("[cap-pub:{id}] FLOOR DENY raw={:02X?}", raw);
                    } else {
                        debug!("[cap-pub:{id}] floor mbcp rx type=0x{:02X} len={}", ty, raw.len());
                    }
                }
                let _ = session.drain_all_events(OP_TRACKS_UPDATE);
            }
        }
    }
    session.close().await;
    Ok(())
}

/// Subscriber 봇 — 방 join + RTP 수신(Count/Full) + WS 펌프(TRACKS_UPDATE→tracks_ready).
pub async fn run_subscriber(
    id: String,
    server: String,
    ws_port: u16,
    room_id: String,
    recv_mode: RecvMode,
    run_window: Duration,
    counters: Arc<CapCounters>,
    latencies: Arc<Mutex<Vec<f64>>>,
    setup_sem: Arc<Semaphore>,
    ready: Arc<AtomicUsize>,
) -> Result<(), DynErr> {
    // ── setup (연결 + STUN/DTLS/SRTP) — semaphore 로 동시 DTLS 제한(storm 회피) + timeout(hang 방지) ──
    let (mut session, sub_socket, mut srtp, server_addr, sub_ufrag, sub_pwd) = {
        let _permit = setup_sem.acquire().await.map_err(|e| format!("sem: {e}"))?;
        let id_s = id.clone();
        let fut = async {
            let session =
                SignalingSession::connect_to_room(&server, ws_port, &id_s, &room_id).await?;
            let sc = session.server_config.clone().ok_or("no server_config")?;
            let server_addr: SocketAddr =
                format!("{}:{}", sc.server_ip, sc.server_port).parse()?;
            let sub_socket = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
            let (_ck, sk, _cs, ss) = media::setup_media_pc(
                &sub_socket, server_addr, &sc.sub_ufrag, &sc.sub_pwd, &format!("sub:{id_s}"),
            )
            .await?;
            let mut srtp = SrtpCtx::new();
            srtp.install(&sk, &ss);
            Ok::<_, DynErr>((session, sub_socket, srtp, server_addr, sc.sub_ufrag.clone(), sc.sub_pwd.clone()))
        };
        match timeout(Duration::from_secs(15), fut).await {
            Ok(Ok(t)) => t,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(format!("[cap-sub:{id}] setup timeout").into()),
        }
    }; // permit drop → 다음 봇 setup 진입

    // setup 완료 신호 → runner 가 전원 ready 확인 후 publisher 발행(trigger). grace 고정값 폐기.
    ready.fetch_add(1, Ordering::Relaxed);

    // sub 는 connect+media 후 즉시 수신 루프(발행 전엔 0). run_window = grace + duration + margin
    // (runner 가 넉넉히 부여 → publisher 발행 window 전체를 덮음).
    let mut buf = vec![0u8; 2048];
    let mut last_seq: HashMap<u32, u16> = HashMap::new();
    let mut pump = interval(Duration::from_millis(200));
    // STUN consent — sub 는 송신 0(수신만)이라 idle 시 서버 reaper(suspect 20s/zombie 35s)에
    // 정리됨 → fan-out 끊김. 주기 STUN binding 으로 peer.last_seen 갱신(liveness 유지).
    let mut consent = interval(Duration::from_secs(3));
    let deadline = Instant::now() + run_window;
    let mut local_lat: Vec<f64> = Vec::new();
    let mut seen_rtp = false; // 첫 fan-out 수신 → active_subs 1회 증가

    loop {
        tokio::select! {
            _ = sleep_until(deadline) => break,
            r = sub_socket.recv_from(&mut buf) => {
                match r {
                    Ok((n, _)) => {
                        let got = handle_rtp(
                            &buf[..n], recv_mode, &mut srtp,
                            &mut last_seq, &counters, &mut local_lat,
                        );
                        if got && !seen_rtp {
                            seen_rtp = true;
                            counters.active_subs.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(e) => { warn!("[cap-sub:{id}] recv error: {e}"); break; }
                }
            }
            _ = consent.tick() => {
                let req = oxlens_lab_common::stun::build_binding_request(
                    &sub_ufrag, &media::random_ice_string(4), &sub_pwd,
                );
                let _ = sub_socket.send_to(&req, server_addr).await;
            }
            _ = pump.tick() => {
                session.poll_events().await;
                // 발행 통지 도착 → TRACKS_READY (SubscriberGate resume → fan-out 시작)
                if !session.drain_all_events(OP_TRACKS_UPDATE).is_empty() {
                    let _ = session.tracks_ready().await;
                }
            }
        }
    }
    session.close().await;

    if !local_lat.is_empty() {
        latencies.lock().await.extend(local_lat);
    }
    Ok(())
}

/// 단일 RTP 패킷 처리. Count = 헤더 평문 파싱만. Full = 복호 + latency.
/// 반환 = RTP(fan-out) 패킷으로 카운트했는지 (STUN/RTCP = false).
fn handle_rtp(
    pkt: &[u8],
    mode: RecvMode,
    srtp: &mut SrtpCtx,
    last_seq: &mut HashMap<u32, u16>,
    counters: &CapCounters,
    local_lat: &mut Vec<f64>,
) -> bool {
    if pkt.len() < 12 || !is_rtp(pkt[0]) {
        return false; // STUN/DTLS/비-RTP
    }
    let pt = pkt[1] & 0x7F;
    if (72..=79).contains(&pt) {
        return false; // RTCP (SR/RR/etc)
    }

    // ── 헤더 12B 는 SRTP 에서도 평문 — 복호 없이 seq/ssrc ──
    let seq = u16::from_be_bytes([pkt[2], pkt[3]]);
    let ssrc = u32::from_be_bytes([pkt[8], pkt[9], pkt[10], pkt[11]]);

    counters.rx_packets.fetch_add(1, Ordering::Relaxed);
    counters.rx_bytes.fetch_add(pkt.len() as u64, Ordering::Relaxed);

    // seq-gap loss (per ssrc)
    if let Some(&prev) = last_seq.get(&ssrc) {
        let expected = prev.wrapping_add(1);
        let diff = seq.wrapping_sub(expected) as i16;
        if diff > 0 {
            counters.rx_lost.fetch_add(diff as u64, Ordering::Relaxed);
        }
    }
    last_seq.insert(ssrc, seq);

    // ── Full 모드만 복호 + latency ──
    if mode == RecvMode::Full {
        match srtp.decrypt_rtp(pkt) {
            Some(plain) => {
                let vpt = if plain.len() > 1 { plain[1] & 0x7F } else { 0 };
                // video(pt!=111) 평문 끝 8B = publisher 가 심은 send ts(µs)
                if vpt != 111 && plain.len() >= 8 {
                    let n = plain.len();
                    let send_us = u64::from_le_bytes(plain[n - 8..].try_into().unwrap_or([0; 8]));
                    if send_us > 0 {
                        let lat = now_micros().saturating_sub(send_us) as f64;
                        // sane guard (음수/거대값 = 클럭/garbage 배제)
                        if lat > 0.0 && lat < 10_000_000.0 {
                            local_lat.push(lat);
                        }
                    }
                }
            }
            None => {
                counters.rx_decrypt_fail.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    true
}
