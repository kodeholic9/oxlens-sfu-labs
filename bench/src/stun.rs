// author: kodeholic (powered by Claude)
//! STUN binding request builder for bench client
//!
//! Server (light-livechat) only builds binding responses.
//! Client needs to build binding requests with:
//!   USERNAME, PRIORITY, ICE-CONTROLLING, USE-CANDIDATE,
//!   MESSAGE-INTEGRITY, FINGERPRINT

/// STUN magic cookie (RFC 8489)
const MAGIC_COOKIE: u32 = 0x2112_A442;
const HEADER_SIZE: usize = 20;

// Message types
const BINDING_REQUEST: u16 = 0x0001;

// Attribute types
const ATTR_USERNAME: u16 = 0x0006;
const ATTR_MESSAGE_INTEGRITY: u16 = 0x0008;
const ATTR_PRIORITY: u16 = 0x0024;
const ATTR_USE_CANDIDATE: u16 = 0x0025;
const ATTR_ICE_CONTROLLING: u16 = 0x802A;
const ATTR_FINGERPRINT: u16 = 0x8028;

/// Build a STUN Binding Request for ICE connectivity check.
///
/// `server_ufrag`: server's ICE ufrag (from server_config)
/// `client_ufrag`: our local ufrag
/// `server_pwd`: server's ICE password (for MESSAGE-INTEGRITY)
pub fn build_binding_request(
    server_ufrag: &str,
    client_ufrag: &str,
    server_pwd: &str,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);

    // Transaction ID (random 12 bytes)
    let mut tid = [0u8; 12];
    getrandom::fill(&mut tid).expect("getrandom failed");

    // ── Header ──
    buf.extend_from_slice(&BINDING_REQUEST.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes()); // length placeholder
    buf.extend_from_slice(&MAGIC_COOKIE.to_be_bytes());
    buf.extend_from_slice(&tid);

    // ── USERNAME: "server_ufrag:client_ufrag" ──
    let username = format!("{}:{}", server_ufrag, client_ufrag);
    write_attr(&mut buf, ATTR_USERNAME, username.as_bytes());

    // ── PRIORITY ──
    // Type preference=126 (host), local preference=65535, component=1
    // priority = (2^24 * 126) + (2^8 * 65535) + (256 - 1) = 2130706431
    let priority: u32 = 2_130_706_431;
    write_attr(&mut buf, ATTR_PRIORITY, &priority.to_be_bytes());

    // ── ICE-CONTROLLING (8-byte tie-breaker) ──
    let mut tie_breaker = [0u8; 8];
    getrandom::fill(&mut tie_breaker).expect("getrandom failed");
    write_attr(&mut buf, ATTR_ICE_CONTROLLING, &tie_breaker);

    // ── USE-CANDIDATE (empty, signals nomination) ──
    write_attr(&mut buf, ATTR_USE_CANDIDATE, &[]);

    // ── MESSAGE-INTEGRITY ──
    let key = server_pwd.as_bytes();
    // Adjust length to include MI attr (4 header + 20 value = 24)
    let len_with_mi = (buf.len() - HEADER_SIZE + 24) as u16;
    buf[2..4].copy_from_slice(&len_with_mi.to_be_bytes());

    let hmac_value = compute_hmac_sha1(&buf, key);
    write_attr(&mut buf, ATTR_MESSAGE_INTEGRITY, &hmac_value);

    // ── FINGERPRINT ──
    let len_with_fp = (buf.len() - HEADER_SIZE + 8) as u16;
    buf[2..4].copy_from_slice(&len_with_fp.to_be_bytes());

    let crc = crc32fast::hash(&buf) ^ 0x5354_554E;
    write_attr(&mut buf, ATTR_FINGERPRINT, &crc.to_be_bytes());

    // Final length
    let final_len = (buf.len() - HEADER_SIZE) as u16;
    buf[2..4].copy_from_slice(&final_len.to_be_bytes());

    buf
}

fn write_attr(buf: &mut Vec<u8>, attr_type: u16, value: &[u8]) {
    buf.extend_from_slice(&attr_type.to_be_bytes());
    buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
    buf.extend_from_slice(value);
    let padding = (4 - (value.len() % 4)) % 4;
    buf.extend(std::iter::repeat(0u8).take(padding));
}

fn compute_hmac_sha1(data: &[u8], key: &[u8]) -> [u8; 20] {
    use hmac::{Hmac, Mac};
    use sha1::Sha1;

    type HmacSha1 = Hmac<Sha1>;
    let mut mac = HmacSha1::new_from_slice(key).expect("HMAC key");
    mac.update(data);
    let result = mac.finalize();
    let bytes = result.into_bytes();
    let mut out = [0u8; 20];
    out.copy_from_slice(&bytes);
    out
}
