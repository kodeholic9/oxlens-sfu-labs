// author: kodeholic (powered by Claude)
//! oxlab-net — Userspace network filter
//!
//! tc/netem이 커널에서 하는 일을 유저스페이스에서 수행.
//! - 크로스 플랫폼 (Linux/macOS/Windows)
//! - 참가자별 개별 프로파일
//! - sudo 불필요
//! - 런타임 동적 전환

pub mod filter;
pub mod profile;

pub use filter::{FilterConfig, FilterResult, NetFilter};
pub use profile::NetworkProfile;
