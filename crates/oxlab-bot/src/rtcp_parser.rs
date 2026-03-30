// author: kodeholic (powered by Claude)
//! RTCP/RTP 파서 유틸리티 — PLI/RR/SR 감지 + VP8 keyframe 판별

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tracing::debug;
use crate::observations::BotObservations;

/// VP8 keyframe 감지 (RTP payload 내 VP8 PD 바이트)
/// VP8 PD: S=1이면 파티션 시작, 이후 바이트 bit 0 = 0이면 keyframe
pub(crate) fn detect_vp8_keyframe(rtp_pkt: &[u8]) -> bool {
    if rtp_pkt.len() < 14 { return false; }
    let cc = (rtp_pkt[0] & 0x0F) as usize;
    let has_ext = (rtp_pkt[0] & 0x10) != 0;
    let mut offset = 12 + cc * 4;
    if has_ext {
        if rtp_pkt.len() < offset + 4 { return false; }
        let ext_len = u16::from_be_bytes([rtp_pkt[offset + 2], rtp_pkt[offset + 3]]) as usize;
        offset += 4 + ext_len * 4;
    }
    if rtp_pkt.len() <= offset + 1 { return false; }
    let pd = rtp_pkt[offset];
    let s_bit = (pd & 0x10) != 0;
    if !s_bit { return false; }
    if rtp_pkt.len() <= offset + 1 { return false; }
    let ph = rtp_pkt[offset + 1];
    (ph & 0x01) == 0
}

/// 간단한 문자열 해시 (SSRC 생성용)
pub(crate) fn simple_hash(s: &str) -> u32 {
    let mut hash: u32 = 5381;
    for b in s.bytes() {
        hash = hash.wrapping_mul(33).wrapping_add(b as u32);
    }
    hash
}

/// RTCP compound 패킷 파싱 — PLI + RR 감지 (publisher 측)
pub(crate) fn parse_rtcp_compound(
    buf: &[u8],
    my_video_ssrc: u32,
    force_keyframe: &Arc<AtomicBool>,
    observations: &BotObservations,
    bot_id: &str,
) {
    let mut offset = 0;
    while offset + 4 <= buf.len() {
        let b0 = buf[offset];
        let pt = buf[offset + 1];
        let length_words = u16::from_be_bytes([buf[offset + 2], buf[offset + 3]]) as usize;
        let pkt_len = (length_words + 1) * 4;
        if offset + pkt_len > buf.len() { break; }

        match pt {
            201 if pkt_len >= 8 => {
                let sender_ssrc = u32::from_be_bytes([
                    buf[offset + 4], buf[offset + 5],
                    buf[offset + 6], buf[offset + 7],
                ]);
                observations.record_rr(sender_ssrc);
                debug!("[bot:{}] RR received, sender_ssrc=0x{:08X}", bot_id, sender_ssrc);
            }
            206 => {
                let fmt = b0 & 0x1F;
                if fmt == 1 && pkt_len >= 12 {
                    let media_ssrc = u32::from_be_bytes([
                        buf[offset + 8], buf[offset + 9],
                        buf[offset + 10], buf[offset + 11],
                    ]);
                    if media_ssrc == my_video_ssrc {
                        debug!("[bot:{}] PLI received for ssrc=0x{:08X}", bot_id, media_ssrc);
                        force_keyframe.store(true, Ordering::Relaxed);
                        observations.record_pli();
                    }
                }
            }
            _ => {}
        }
        offset += pkt_len;
    }
}

/// subscriber 측 RTCP compound 파싱 — SR 감지 (L1-02, L1-03)
pub(crate) fn parse_rtcp_for_sr(
    buf: &[u8],
    observations: &BotObservations,
    bot_id: &str,
) {
    let mut offset = 0;
    while offset + 4 <= buf.len() {
        let pt = buf[offset + 1];
        let length_words = u16::from_be_bytes([buf[offset + 2], buf[offset + 3]]) as usize;
        let pkt_len = (length_words + 1) * 4;
        if offset + pkt_len > buf.len() { break; }

        if pt == 200 && pkt_len >= 28 {
            let ssrc = u32::from_be_bytes([buf[offset+4], buf[offset+5], buf[offset+6], buf[offset+7]]);
            let ntp_sec = u32::from_be_bytes([buf[offset+8], buf[offset+9], buf[offset+10], buf[offset+11]]);
            let ntp_frac = u32::from_be_bytes([buf[offset+12], buf[offset+13], buf[offset+14], buf[offset+15]]);
            let rtp_ts = u32::from_be_bytes([buf[offset+16], buf[offset+17], buf[offset+18], buf[offset+19]]);
            let pkt_count = u32::from_be_bytes([buf[offset+20], buf[offset+21], buf[offset+22], buf[offset+23]]);
            let oct_count = u32::from_be_bytes([buf[offset+24], buf[offset+25], buf[offset+26], buf[offset+27]]);
            observations.record_sr(ssrc, ntp_sec, ntp_frac, rtp_ts, pkt_count, oct_count);
            debug!("[bot:{}] SR received ssrc=0x{:08X} ntp={}.{} rtp_ts={}",
                bot_id, ssrc, ntp_sec, ntp_frac, rtp_ts);
        }
        offset += pkt_len;
    }
}
