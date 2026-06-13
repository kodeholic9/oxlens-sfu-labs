// author: kodeholic (powered by Claude)
//! oxlab-bot — SFU 테스트용 트래픽 봇
//!
//! Phase 0: WS 연결 → IDENTIFY → ROOM_JOIN (시그널링만)
//! Phase 1: STUN+DTLS+SRTP 미디어 셋업 + Fake RTP 송수신

pub mod bot;
pub mod cap;
pub mod observations;
pub(crate) mod rtcp_parser;
pub mod rtp_publisher;
pub mod rtp_subscriber;

pub use bot::{Bot, BotConfig, BotStatus, MediaContext};
pub use cap::{CapCounters, RecvMode, run_conf_bot, run_publisher, run_subscriber};
pub use observations::{BotObservations, ObservationsInner, SrRecord, FloorGrantRecord, TsGapRecord, LayerSwitchEvent};
pub use rtp_publisher::{RtpPublisherConfig, RtpPublisherState};
pub use rtp_subscriber::{RecvMetricsStore, RecvSnapshot};
