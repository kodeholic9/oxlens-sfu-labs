// author: kodeholic (powered by Claude)
//! oxlab-bot — SFU 테스트용 트래픽 봇
//!
//! Phase 0: WS 연결 → IDENTIFY → ROOM_JOIN (시그널링만)
//! Phase 1 예정: Fake RTP Publisher/Subscriber, PTT 봇

pub mod bot;

pub use bot::{Bot, BotConfig, BotStatus};
