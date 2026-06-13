// author: kodeholic (powered by Claude)
//! Bot — SFU에 접속하는 가짜 참가자
//!
//! Phase 0: 시그널링(WS) 접속 + 방 입장까지
//! Phase 1: STUN+DTLS+SRTP 미디어 셋업 + Fake RTP 송수신 + PLI 응답

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use oxlab_net::{FilterResult, NetFilter};
use oxlens_lab_common::media::{self, SrtpCtx, is_rtp};
use std::collections::{HashMap, HashSet};
use oxlens_lab_common::signaling::{
    SignalingSession, OP_PUBLISH_TRACKS, OP_TRACKS_UPDATE, OP_ROOM_EVENT,
};
use oxlens_lab_common::mbcp::{self, SUBTYPE_FTKN, SUBTYPE_FIDL, SUBTYPE_FRVK};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::rtp_publisher::{
    build_publish_intent_json, RtpPublisherConfig, RtpPublisherState,
};
use crate::observations::BotObservations;
use crate::rtp_subscriber::{
    RecvMetricsStore, RecvSnapshot, format_recv_summary,
};

// ── Bot 상태 ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BotStatus {
    Created,
    Connected,
    Joined,
    MediaReady,
    Publishing,
    Failed,
    Stopped,
}

// ── Floor 상태 공유 (recv task ↔ WS event handler) ──

#[derive(Clone)]
pub struct SharedFloorState {
    inner: Arc<SharedFloorStateInner>,
}

struct SharedFloorStateInner {
    /// true = nobody talking (L1-04)
    floor_idle: AtomicBool,
    /// true after speaker change, reset after first video keyframe (L1-06)
    speaker_just_changed: AtomicBool,
    /// true after floor release, reset on next FLOOR_TAKEN (L1-05)
    floor_just_released: AtomicBool,
    /// ACK sent flag for recv task (L1-11)
    ack_sent: AtomicBool,
}

impl SharedFloorState {
    fn new() -> Self {
        Self {
            inner: Arc::new(SharedFloorStateInner {
                floor_idle: AtomicBool::new(true),
                speaker_just_changed: AtomicBool::new(false),
                floor_just_released: AtomicBool::new(false),
                ack_sent: AtomicBool::new(false),
            }),
        }
    }
}

// ── Simulcast Layer Switch 관측 (L1-18~20) ──

#[derive(Clone)]
struct PendingLayerSwitch {
    requested_layer: String,
    request_time: tokio::time::Instant,
    ts_before: Option<u32>,
    done: bool,
}

// ── Bot 설정 ──

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotConfig {
    pub id: String,
    pub server: String,
    #[serde(default = "default_ws_port")]
    pub ws_port: u16,
    pub room_name: String,
    #[serde(default = "default_mode")]
    pub mode: String,
    pub profile: Option<String>,
}

fn default_ws_port() -> u16 { 1974 }
fn default_mode() -> String { "conference".to_string() }

// ── SRTP Keying Material ──

#[derive(Debug, Clone)]
struct SrtpKeys {
    key: Vec<u8>,
    salt: Vec<u8>,
}

impl SrtpKeys {
    fn new_srtp_ctx(&self) -> SrtpCtx {
        let mut ctx = SrtpCtx::new();
        ctx.install(&self.key, &self.salt);
        ctx
    }
}

// ── 미디어 컨텍스트 ──

pub struct MediaContext {
    pub pub_socket: Arc<UdpSocket>,
    pub sub_socket: Arc<UdpSocket>,
    pub pub_srtp: SrtpCtx,
    pub sub_srtp: SrtpCtx,
    pub server_addr: SocketAddr,
    /// publish SRTP: client_key (봇→서버 encrypt)
    pub_client_keys: SrtpKeys,
    /// publish SRTP: server_key (서버→봇 RTCP decrypt — PLI 수신용)
    pub_server_keys: SrtpKeys,
    /// subscribe SRTP: server_key (서버→봇 RTP decrypt)
    sub_server_keys: SrtpKeys,
}

// ── Bot 본체 ──

pub struct Bot {
    pub config: BotConfig,
    pub status: BotStatus,
    pub user_id: Option<String>,
    pub room_id: Option<String>,
    session: Option<SignalingSession>,
    pub media: Option<MediaContext>,
    pub rtp_config: RtpPublisherConfig,
    pub recv_metrics: RecvMetricsStore,
    /// Layer 1 관측 데이터
    pub observations: BotObservations,
    /// Media task handles
    task_handles: Vec<JoinHandle<()>>,
    /// 전체 미디어 task 중단 시그널
    media_cancel: Option<tokio::sync::watch::Sender<bool>>,
    net_filter: Option<Arc<Mutex<NetFilter>>>,
    /// 다른 참여자들의 subscribe SSRC 목록 (TRACKS_ACK 응답용)
    subscribe_ssrcs: HashSet<u32>,
    /// Floor state shared with recv tasks
    floor_state: SharedFloorState,
    /// Pending simulcast layer switch (L1-18~20)
    pending_layer_switch: Arc<Mutex<Option<PendingLayerSwitch>>>,
}

impl Bot {
    pub fn new(config: BotConfig, net_filter: Option<NetFilter>) -> Self {
        let id_hash = crate::rtcp_parser::simple_hash(&config.id);
        let rtp_config = RtpPublisherConfig {
            audio_ssrc: 0xA000_0000 | (id_hash & 0x00FF_FFFF),
            video_ssrc: 0xB000_0000 | (id_hash & 0x00FF_FFFF),
            ..RtpPublisherConfig::default()
        };

        let net_filter = net_filter.map(|f| Arc::new(Mutex::new(f)));

        Self {
            config,
            status: BotStatus::Created,
            user_id: None,
            room_id: None,
            session: None,
            media: None,
            rtp_config,
            recv_metrics: RecvMetricsStore::new(),
            observations: BotObservations::new(),
            task_handles: Vec::new(),
            media_cancel: None,
            net_filter,
            subscribe_ssrcs: HashSet::new(),
            floor_state: SharedFloorState::new(),
            pending_layer_switch: Arc::new(Mutex::new(None)),
        }
    }

    // ── 시그널링 ──

    pub async fn connect_and_join(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("[bot:{}] connecting to {}:{}", self.config.id, self.config.server, self.config.ws_port);

        let session = SignalingSession::connect(
            &self.config.server, self.config.ws_port,
            &self.config.id, &self.config.room_name, &self.config.mode,
        ).await?;

        let user_id = session.user_id.clone();
        let room_id = session.room_id.clone();
        info!("[bot:{}] joined room={} user_id={}", self.config.id, room_id, user_id);

        self.user_id = Some(user_id);
        self.room_id = Some(room_id);
        self.session = Some(session);
        self.status = BotStatus::Joined;
        Ok(())
    }

    pub async fn join_existing_room(
        &mut self, room_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("[bot:{}] joining existing room={}", self.config.id, room_id);

        let session = SignalingSession::connect_to_room(
            &self.config.server, self.config.ws_port, &self.config.id, room_id,
        ).await?;

        let user_id = session.user_id.clone();
        let room_id = session.room_id.clone();
        info!("[bot:{}] joined user_id={}", self.config.id, user_id);

        self.user_id = Some(user_id);
        self.room_id = Some(room_id);
        self.session = Some(session);
        self.status = BotStatus::Joined;
        Ok(())
    }

    // ── 미디어 셋업 ──

    pub async fn setup_media(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.status != BotStatus::Joined {
            return Err(format!("setup_media requires Joined, current={:?}", self.status).into());
        }

        let session = self.session.as_ref().ok_or("no signaling session")?;
        let sc = session.server_config.as_ref().ok_or("no server_config")?;

        let server_addr: SocketAddr = format!("{}:{}", sc.server_ip, sc.server_port)
            .parse().map_err(|e| format!("parse server addr: {e}"))?;

        let bot_id = &self.config.id;

        // ── Publish PC ──
        let pub_socket = Arc::new(
            UdpSocket::bind("0.0.0.0:0").await.map_err(|e| format!("pub bind: {e}"))?
        );
        let (pub_client_key, pub_server_key, pub_client_salt, pub_server_salt) =
            media::setup_media_pc(
                &pub_socket, server_addr,
                &sc.pub_ufrag, &sc.pub_pwd,
                &format!("pub:{}", bot_id),
            ).await.map_err(|e| format!("pub media setup: {e}"))?;

        let pub_client_keys = SrtpKeys { key: pub_client_key.clone(), salt: pub_client_salt.clone() };
        let pub_server_keys = SrtpKeys { key: pub_server_key, salt: pub_server_salt };

        let mut pub_srtp = SrtpCtx::new();
        pub_srtp.install(&pub_client_key, &pub_client_salt);
        info!("[bot:{}] publish PC ready", bot_id);

        // ── Subscribe PC ──
        let sub_socket = Arc::new(
            UdpSocket::bind("0.0.0.0:0").await.map_err(|e| format!("sub bind: {e}"))?
        );
        let (_sub_client_key, sub_server_key, _sub_client_salt, sub_server_salt) =
            media::setup_media_pc(
                &sub_socket, server_addr,
                &sc.sub_ufrag, &sc.sub_pwd,
                &format!("sub:{}", bot_id),
            ).await.map_err(|e| format!("sub media setup: {e}"))?;

        let sub_server_keys = SrtpKeys { key: sub_server_key.clone(), salt: sub_server_salt.clone() };

        let mut sub_srtp = SrtpCtx::new();
        sub_srtp.install(&sub_server_key, &sub_server_salt);
        info!("[bot:{}] subscribe PC ready", bot_id);

        self.media = Some(MediaContext {
            pub_socket, sub_socket, pub_srtp, sub_srtp, server_addr,
            pub_client_keys, pub_server_keys, sub_server_keys,
        });
        self.status = BotStatus::MediaReady;
        info!("[bot:{}] media ready (STUN+DTLS+SRTP)", bot_id);
        Ok(())
    }

    // ── PUBLISH_TRACKS intent ──

    pub async fn publish_intent(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // ptt 모드 = half-duplex(floor gating), 그 외(broadcast/conference) = full.
        let duplex = if self.config.mode == "ptt" { "half" } else { "full" };
        let payload = build_publish_intent_json(&self.rtp_config, duplex);
        let session = self.session.as_mut().ok_or("no signaling session")?;
        info!("[bot:{}] sending PUBLISH_TRACKS intent: {}", self.config.id, payload);

        let resp = session.request_routed(OP_PUBLISH_TRACKS, payload).await?;
        if resp.ok != Some(true) {
            return Err(format!("PUBLISH_TRACKS failed: {:?}", resp.d).into());
        }
        info!("[bot:{}] PUBLISH_TRACKS intent OK", self.config.id);
        Ok(())
    }

    // ── Publishing + Subscribing ──

    pub fn start_publishing(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.status != BotStatus::MediaReady {
            return Err(format!("start_publishing requires MediaReady, current={:?}", self.status).into());
        }

        let media = self.media.as_ref().ok_or("no media context")?;
        let (cancel_tx, cancel_rx_audio) = tokio::sync::watch::channel(false);
        let cancel_rx_video = cancel_tx.subscribe();
        let cancel_rx_sub = cancel_tx.subscribe();
        let cancel_rx_pub = cancel_tx.subscribe();

        let server_addr = media.server_addr;
        let rtp_config = self.rtp_config.clone();
        let bot_id = self.config.id.clone();

        // PLI 응답용 공유 플래그
        let force_keyframe = Arc::new(AtomicBool::new(false));

        // ── 1. Audio publish task (20ms) ──
        let audio_socket = Arc::clone(&media.pub_socket);
        let audio_keys = media.pub_client_keys.clone();
        let audio_config = rtp_config.clone();
        let audio_bot_id = bot_id.clone();
        let audio_filter = self.net_filter.clone();
        let audio_handle = tokio::spawn(async move {
            let mut srtp = audio_keys.new_srtp_ctx();
            let mut state = RtpPublisherState::new(audio_config);
            let mut cancel = cancel_rx_audio;
            let mut interval = tokio::time::interval(Duration::from_millis(20));

            info!("[bot:{}] audio publish loop started", audio_bot_id);
            loop {
                tokio::select! {
                    _ = cancel.changed() => { if *cancel.borrow() { break; } }
                    _ = interval.tick() => {
                        let pkt = state.build_opus_packet();
                        if let Some(encrypted) = srtp.encrypt_rtp(&pkt) {
                            let filter_result = audio_filter.as_ref()
                                .map(|f| f.lock().unwrap().filter(encrypted.len()));
                            match filter_result {
                                Some(FilterResult::Drop) => continue,
                                Some(FilterResult::Pass { delay }) if !delay.is_zero() => {
                                    tokio::time::sleep(delay).await;
                                }
                                _ => {}
                            }
                            if let Err(e) = audio_socket.send_to(&encrypted, server_addr).await {
                                warn!("[bot:{}] audio send error: {}", audio_bot_id, e);
                                break;
                            }
                        }
                    }
                }
            }
            debug!("[bot:{}] audio publish loop ended", audio_bot_id);
        });

        // ── 2. Video publish task (33ms) ──
        let video_socket = Arc::clone(&media.pub_socket);
        let video_keys = media.pub_client_keys.clone();
        let video_config = rtp_config.clone();
        let video_bot_id = bot_id.clone();
        let video_filter = self.net_filter.clone();
        let video_kf_flag = Arc::clone(&force_keyframe);
        let video_handle = tokio::spawn(async move {
            let mut srtp = video_keys.new_srtp_ctx();
            let mut state = RtpPublisherState::new_with_keyframe_flag(video_config, video_kf_flag);
            let mut cancel = cancel_rx_video;
            let mut interval = tokio::time::interval(Duration::from_millis(33));

            info!("[bot:{}] video publish loop started", video_bot_id);
            loop {
                tokio::select! {
                    _ = cancel.changed() => { if *cancel.borrow() { break; } }
                    _ = interval.tick() => {
                        let pkt = state.build_vp8_packet();
                        if let Some(encrypted) = srtp.encrypt_rtp(&pkt) {
                            let filter_result = video_filter.as_ref()
                                .map(|f| f.lock().unwrap().filter(encrypted.len()));
                            match filter_result {
                                Some(FilterResult::Drop) => continue,
                                Some(FilterResult::Pass { delay }) if !delay.is_zero() => {
                                    tokio::time::sleep(delay).await;
                                }
                                _ => {}
                            }
                            if let Err(e) = video_socket.send_to(&encrypted, server_addr).await {
                                warn!("[bot:{}] video send error: {}", video_bot_id, e);
                                break;
                            }
                        }
                    }
                }
            }
            debug!("[bot:{}] video publish loop ended", video_bot_id);
        });

        // ── 3. Pub socket recv task (PLI 감지) ──
        let pub_recv_socket = Arc::clone(&media.pub_socket);
        let pub_recv_keys = media.pub_server_keys.clone();
        let pub_recv_bot_id = bot_id.clone();
        let pub_pli_flag = Arc::clone(&force_keyframe);
        let pub_video_ssrc = self.rtp_config.video_ssrc;
        let pub_observations = self.observations.clone();
        let pub_recv_handle = tokio::spawn(async move {
            let mut srtp = pub_recv_keys.new_srtp_ctx();
            let mut cancel = cancel_rx_pub;
            let mut buf = vec![0u8; 2048];

            debug!("[bot:{}] pub recv loop started (PLI listener)", pub_recv_bot_id);
            let mut pli_count: u32 = 0;
            let mut rtcp_count: u32 = 0;
            let mut decrypt_fail: u32 = 0;
            loop {
                tokio::select! {
                    _ = cancel.changed() => { if *cancel.borrow() { break; } }
                    result = pub_recv_socket.recv_from(&mut buf) => {
                        match result {
                            Ok((n, _)) => {
                                if n < 12 { continue; }
                                rtcp_count += 1;
                                match srtp.decrypt_rtcp(&buf[..n]) {
                                    Some(plaintext) => {
                                        let prev = pub_pli_flag.load(Ordering::Relaxed);
                                        crate::rtcp_parser::parse_rtcp_compound(
                                            &plaintext, pub_video_ssrc,
                                            &pub_pli_flag, &pub_observations,
                                            &pub_recv_bot_id,
                                        );
                                        if !prev && pub_pli_flag.load(Ordering::Relaxed) {
                                            pli_count += 1;
                                            info!("[bot:{}] PLI #{} detected, keyframe scheduled",
                                                pub_recv_bot_id, pli_count);
                                        }
                                    }
                                    None => {
                                        decrypt_fail += 1;
                                        if decrypt_fail <= 3 {
                                            debug!("[bot:{}] pub SRTCP decrypt failed ({}/{}B) fails={}",
                                                pub_recv_bot_id, rtcp_count, n, decrypt_fail);
                                        }
                                    }
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            info!("[bot:{}] pub recv loop ended: rtcp={} pli={} decrypt_fail={}",
                pub_recv_bot_id, rtcp_count, pli_count, decrypt_fail);
            debug!("[bot:{}] pub recv loop ended", pub_recv_bot_id);
        });

        // ── 4. Subscribe recv task (RTP 메트릭 + L1 observation) ──
        let sub_socket = Arc::clone(&media.sub_socket);
        let sub_keys = media.sub_server_keys.clone();
        let metrics_store = self.recv_metrics.clone();
        let sub_bot_id = bot_id.clone();
        let sub_observations = self.observations.clone();
        let sub_floor_state = self.floor_state.clone();
        let sub_mode = self.config.mode.clone();
        let sub_pending_switch = Arc::clone(&self.pending_layer_switch);
        let sub_handle = tokio::spawn(async move {
            let mut srtp = sub_keys.new_srtp_ctx();
            let mut cancel = cancel_rx_sub;
            let mut buf = vec![0u8; 2048];

            let mut ssrc_last: HashMap<u32, (tokio::time::Instant, u32)> = HashMap::new();
            info!("[bot:{}] subscribe recv loop started", sub_bot_id);
            loop {
                tokio::select! {
                    _ = cancel.changed() => { if *cancel.borrow() { break; } }
                    result = sub_socket.recv_from(&mut buf) => {
                        match result {
                            Ok((n, _addr)) => {
                                if n < 12 { continue; }
                                let b1 = buf[1];
                                let pt = b1 & 0x7F;

                                if is_rtp(buf[0]) && !(72..=79).contains(&pt) {
                                    // RTP
                                    let plaintext = match srtp.decrypt_rtp(&buf[..n]) {
                                        Some(p) => p,
                                        None => continue,
                                    };
                                    if plaintext.len() < 12 { continue; }
                                    let seq = u16::from_be_bytes([plaintext[2], plaintext[3]]);
                                    let ts = u32::from_be_bytes([plaintext[4], plaintext[5], plaintext[6], plaintext[7]]);
                                    let ssrc = u32::from_be_bytes([plaintext[8], plaintext[9], plaintext[10], plaintext[11]]);
                                    let rtp_pt = plaintext[1] & 0x7F;
                                    metrics_store.update(ssrc, rtp_pt, seq, ts);

                                    // L1-07: PTT SSRC 추적 (audio=111, video=그 외)
                                    if rtp_pt == 111 {
                                        sub_observations.record_ptt_audio_ssrc(ssrc);
                                    } else {
                                        sub_observations.record_ptt_video_ssrc(ssrc);
                                    }

                                    // L1-04: PTT non-speaker gating
                                    if sub_mode == "ptt" && sub_floor_state.inner.floor_idle.load(Ordering::Relaxed) {
                                        sub_observations.record_ptt_non_speaker_rtp();
                                    }

                                    // L1-05: silence flush (Opus silence = PT 111, payload <= 3 bytes after header)
                                    if sub_mode == "ptt" && sub_floor_state.inner.floor_just_released.load(Ordering::Relaxed) {
                                        if rtp_pt == 111 && plaintext.len() >= 12 {
                                            let payload_len = plaintext.len() - 12; // approximate, ignoring CSRC/ext
                                            if payload_len <= 3 {
                                                sub_observations.record_ptt_silence_frame();
                                            }
                                        }
                                    }

                                    // L1-06: first video after speaker change = keyframe?
                                    if sub_mode == "ptt" && rtp_pt != 111
                                        && sub_floor_state.inner.speaker_just_changed.swap(false, Ordering::Relaxed)
                                    {
                                        let is_kf = crate::rtcp_parser::detect_vp8_keyframe(&plaintext);
                                        sub_observations.record_ptt_first_video_keyframe(is_kf);
                                    }

                                    // L1-11: video before ACK (non-audio only)
                                    if rtp_pt != 111 && !sub_floor_state.inner.ack_sent.load(Ordering::Relaxed) {
                                        sub_observations.record_video_before_ack();
                                    }

                                    // L1-08: PTT ts_gap tracking (idle > 1s gap)
                                    {
                                        let now = tokio::time::Instant::now();
                                        if sub_mode == "ptt" {
                                            if let Some(&(prev_time, prev_ts)) = ssrc_last.get(&ssrc) {
                                                let wall_gap_ms = now.duration_since(prev_time).as_millis() as u64;
                                                if wall_gap_ms > 1000 {
                                                    let rtp_ts_gap = ts.wrapping_sub(prev_ts);
                                                    let sample_rate = if rtp_pt == 111 { 48000u32 } else { 90000u32 };
                                                    sub_observations.record_ts_gap(ssrc, wall_gap_ms, rtp_ts_gap, sample_rate);
                                                }
                                            }
                                        }
                                        ssrc_last.insert(ssrc, (now, ts)); // 모든 모드에서 업데이트 (L1-18~20 용)
                                    }

                                    // L1-18~20: Simulcast layer switch observation
                                    if rtp_pt != 111 { // video only
                                        if let Ok(mut pending) = sub_pending_switch.lock() {
                                            if let Some(ref mut sw) = *pending {
                                                if !sw.done {
                                                    // ts_before: 이전에 기록된 video ts 사용
                                                    if sw.ts_before.is_none() {
                                                        sw.ts_before = ssrc_last.values()
                                                            .map(|&(_, t)| t)
                                                            .next();
                                                    }
                                                    let is_kf = crate::rtcp_parser::detect_vp8_keyframe(&plaintext);
                                                    let elapsed = sw.request_time.elapsed().as_millis() as u64;
                                                    let event = crate::observations::LayerSwitchEvent {
                                                        requested_layer: sw.requested_layer.clone(),
                                                        request_timestamp_ms: elapsed,
                                                        keyframe_after_ms: if is_kf { Some(elapsed) } else { None },
                                                        ts_before: sw.ts_before,
                                                        ts_after: Some(ts),
                                                        packets_after: true,
                                                    };
                                                    sub_observations.record_layer_switch(event);
                                                    sw.done = true;
                                                }
                                            }
                                        }
                                    }
                                } else {
                                    // RTCP — SR 파싱 (L1-02, L1-03)
                                    if let Some(plaintext) = srtp.decrypt_rtcp(&buf[..n]) {
                                        crate::rtcp_parser::parse_rtcp_for_sr(
                                            &plaintext, &sub_observations, &sub_bot_id,
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("[bot:{}] sub recv error: {}", sub_bot_id, e);
                                break;
                            }
                        }
                    }
                }
            }
            debug!("[bot:{}] subscribe recv loop ended", sub_bot_id);
        });

        self.task_handles = vec![audio_handle, video_handle, pub_recv_handle, sub_handle];
        self.media_cancel = Some(cancel_tx);
        self.status = BotStatus::Publishing;

        info!("[bot:{}] publishing + subscribing started (4 tasks)", bot_id);
        Ok(())
    }

    // ── 네트워크 필터 ──

    /// 네트워크 필터 동적 전환 (시나리오 엔진이 호출)
    pub fn update_net_filter(&self, config: oxlab_net::FilterConfig) {
        if let Some(ref filter) = self.net_filter {
            if let Ok(mut f) = filter.lock() {
                f.update(config);
            }
        }
    }

    // ── PTT Floor Control ──

    /// Floor Request — v3 FLOOR_MBCP (WS binary fallback) FREQ 송신 + FTKN 대기.
    /// 단일 화자 capacity 에선 경쟁 없이 즉시 grant. 시그니처는 시나리오 호환 보존.
    pub async fn floor_request_ws(&mut self, priority: u8) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let ssrc = self.rtp_config.audio_ssrc;
        let pkt = mbcp::build_freq(ssrc);
        {
            let session = self.session.as_mut().ok_or("no session")?;
            session.send_floor_mbcp(&pkt).await?;
        }
        info!("[bot:{}] MBCP FREQ (floor request) ssrc=0x{:08X}", self.config.id, ssrc);

        let granted = self.await_floor_taken(Duration::from_secs(1)).await;
        // L1-14: floor grant 순서 기록 (queued 개념 폐기 — MBCP 즉시성)
        self.observations.record_floor_grant(&self.config.id, priority, granted, false);
        Ok(granted)
    }

    /// FLOOR_MBCP FTKN 도착 대기 (timeout). floor 획득 확인.
    async fn await_floor_taken(&mut self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            if let Some(session) = self.session.as_mut() {
                session.poll_events().await;
                for raw in session.drain_floor_mbcp() {
                    if let Some(msg) = mbcp::parse(&raw) {
                        if msg.subtype == SUBTYPE_FTKN {
                            self.floor_state.inner.floor_idle.store(false, Ordering::Relaxed);
                            return true;
                        }
                    }
                }
            } else {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Simulcast SUBSCRIBE_LAYER 전송 (L1-18~20 관측 시작)
    pub async fn subscribe_layer(&mut self, layer: &str, ssrcs: &[u32]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let session = self.session.as_mut().ok_or("no session")?;
        let targets: Vec<serde_json::Value> = ssrcs.iter().map(|&ssrc| {
            serde_json::json!({ "ssrc": ssrc, "layer": layer })
        }).collect();

        // pending 설정 → recv task가 관측 (ts_before는 recv task에서 채움)
        if let Ok(mut pending) = self.pending_layer_switch.lock() {
            *pending = Some(PendingLayerSwitch {
                requested_layer: layer.to_string(),
                request_time: tokio::time::Instant::now(),
                ts_before: None,
                done: false,
            });
        }

        let resp = session.subscribe_layer(targets).await?;
        info!("[bot:{}] subscribe_layer layer={} ssrcs={:?} ok={:?}",
            self.config.id, layer, ssrcs, resp.ok);
        Ok(())
    }

    /// Floor Release — v3 FLOOR_MBCP (WS binary) FREL 송신.
    pub async fn floor_release_ws(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ssrc = self.rtp_config.audio_ssrc;
        let pkt = mbcp::build_frel(ssrc);
        let session = self.session.as_mut().ok_or("no session")?;
        session.send_floor_mbcp(&pkt).await?;
        info!("[bot:{}] MBCP FREL (floor release)", self.config.id);
        Ok(())
    }

    /// MBCP Floor Request via UDP (RTCP APP subtype=0)
    pub async fn send_mbcp_freq(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ssrc = self.rtp_config.audio_ssrc;
        let pkt = oxlens_lab_common::mbcp::build_freq(ssrc);
        info!("[bot:{}] MBCP FREQ ssrc=0x{:08X}", self.config.id, ssrc);
        self.send_mbcp(&pkt).await
    }

    /// MBCP Floor Release via UDP (RTCP APP subtype=1)
    pub async fn send_mbcp_frel(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let ssrc = self.rtp_config.audio_ssrc;
        let pkt = oxlens_lab_common::mbcp::build_frel(ssrc);
        info!("[bot:{}] MBCP FREL ssrc=0x{:08X}", self.config.id, ssrc);
        self.send_mbcp(&pkt).await
    }

    /// MBCP 패킷 전송 (SRTCP encrypt + pub socket)
    async fn send_mbcp(&mut self, pkt: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let media = self.media.as_mut().ok_or("no media context")?;
        let encrypted = media.pub_srtp.encrypt_rtcp(pkt)
            .ok_or("SRTCP encrypt failed")?;
        media.pub_socket.send_to(&encrypted, media.server_addr).await?;
        Ok(())
    }

    // ── 이벤트 처리 ──

    /// WS 이벤트 버퍼를 폴하고, TRACKS_UPDATE(add/remove) + Floor/Room 이벤트 처리.
    pub async fn process_events(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 1) WS 수신 버퍼 채우기 + TRACKS_UPDATE drain (session borrow scope)
        let events = {
            let session = match self.session.as_mut() {
                Some(s) => s,
                None => return Ok(()),
            };
            session.poll_events().await;
            session.drain_all_events(OP_TRACKS_UPDATE)
        }; // session borrow 해제

        // 2) subscribe_ssrcs 갱신 (session borrow 없음)
        let mut changed = false;
        for evt in &events {
            let action = evt.d.get("action").and_then(|v| v.as_str()).unwrap_or("");
            let tracks = match evt.d.get("tracks").and_then(|v| v.as_array()) {
                Some(t) => t,
                None => continue,
            };

            for track in tracks {
                let ssrc = match track.get("ssrc").and_then(|v| v.as_u64()) {
                    Some(s) => s as u32,
                    None => continue,
                };

                match action {
                    "add" => {
                        if self.subscribe_ssrcs.insert(ssrc) {
                            changed = true;
                            let kind = track.get("kind").and_then(|v| v.as_str()).unwrap_or("?");
                            let user_id = track.get("user_id").and_then(|v| v.as_str()).unwrap_or("?");
                            info!("[bot:{}] TRACKS_UPDATE add user={} kind={} ssrc=0x{:08X}",
                                self.config.id, user_id, kind, ssrc);
                        }
                    }
                    "remove" => {
                        if self.subscribe_ssrcs.remove(&ssrc) {
                            changed = true;
                            info!("[bot:{}] TRACKS_UPDATE remove ssrc=0x{:08X}",
                                self.config.id, ssrc);
                        }
                    }
                    _ => {}
                }
            }
        }

        // 3) 변경 있으면 TRACKS_READY 전송 (구 TRACKS_ACK — SubscriberGate resume + PLI)
        if changed {
            let sub_count = self.subscribe_ssrcs.len();
            info!("[bot:{}] sending TRACKS_READY (subs={})", self.config.id, sub_count);

            let session = self.session.as_mut().unwrap();
            let resp = session.tracks_ready().await?;
            debug!("[bot:{}] TRACKS_READY ok={:?}", self.config.id, resp.ok);

            // L1-11: SubscriberGate ACK 완료 마킹
            self.observations.mark_tracks_ack_sent();
            self.floor_state.inner.ack_sent.store(true, Ordering::Relaxed);
        }

        // ── Floor events (L1-15, L1-17 관측) ──
        {
            let session = match self.session.as_mut() {
                Some(s) => s,
                None => return Ok(()),
            };

            // v3 Floor = FLOOR_MBCP (0x2400) self-describing TLV (FTKN/FIDL/FRVK)
            for raw in session.drain_floor_mbcp() {
                let Some(msg) = mbcp::parse(&raw) else { continue };
                match msg.subtype {
                    SUBTYPE_FTKN => {
                        self.floor_state.inner.floor_idle.store(false, Ordering::Relaxed);
                        self.floor_state.inner.speaker_just_changed.store(true, Ordering::Relaxed);
                        self.floor_state.inner.floor_just_released.store(false, Ordering::Relaxed);
                        debug!("[bot:{}] MBCP FTKN ssrc=0x{:08X}", self.config.id, msg.ssrc);
                    }
                    SUBTYPE_FIDL => {
                        self.floor_state.inner.floor_idle.store(true, Ordering::Relaxed);
                        self.floor_state.inner.floor_just_released.store(true, Ordering::Relaxed);
                        debug!("[bot:{}] MBCP FIDL", self.config.id);
                    }
                    SUBTYPE_FRVK => {
                        self.observations.record_floor_revoke();
                        debug!("[bot:{}] MBCP FRVK", self.config.id);
                    }
                    _ => {}
                }
            }

            // ROOM_EVENT leave → L1-17 zombie cleanup
            for evt in session.drain_all_events(OP_ROOM_EVENT) {
                let action = evt.d.get("action").and_then(|v| v.as_str()).unwrap_or("");
                if action == "leave" {
                    if let Some(uid) = evt.d.get("user_id").and_then(|v| v.as_str()) {
                        self.observations.record_zombie_cleanup(uid);
                        debug!("[bot:{}] ROOM_EVENT leave user={}", self.config.id, uid);
                    }
                }
            }
        }

        Ok(())
    }

    // ── 메트릭 ──

    pub fn recv_snapshot(&self) -> RecvSnapshot {
        self.recv_metrics.snapshot()
    }

    pub fn log_recv_metrics(&self) {
        let snap = self.recv_snapshot();
        info!("{}", format_recv_summary(&self.config.id, &snap));
    }

    // ── 정리 ──

    pub async fn stop_publishing(&mut self) {
        if let Some(cancel) = self.media_cancel.take() {
            let _ = cancel.send(true);
        }
        // cancel 신호 후 task들이 graceful exit할 시간 부여 (통계 로그 출력)
        tokio::time::sleep(Duration::from_millis(100)).await;
        for h in self.task_handles.drain(..) {
            h.abort();
        }
        if self.status == BotStatus::Publishing {
            self.status = BotStatus::MediaReady;
        }
        info!("[bot:{}] media tasks stopped", self.config.id);
    }

    pub async fn heartbeat(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(ref mut session) = self.session {
            session.heartbeat().await?;
        }
        Ok(())
    }

    pub fn session_mut(&mut self) -> Option<&mut SignalingSession> {
        self.session.as_mut()
    }

    pub async fn disconnect(&mut self) {
        self.stop_publishing().await;
        if let Some(ref filter) = self.net_filter {
            if let Ok(f) = filter.lock() {
                if f.stats_total > 0 {
                    info!(
                        "[bot:{}] netfilter: total={} dropped={} rate={:.1}%",
                        self.config.id, f.stats_total, f.stats_dropped, f.drop_rate()
                    );
                }
            }
        }
        if let Some(ref mut session) = self.session {
            session.close().await;
        }
        self.status = BotStatus::Stopped;
        info!("[bot:{}] disconnected", self.config.id);
    }

    pub fn id(&self) -> &str {
        &self.config.id
    }
}