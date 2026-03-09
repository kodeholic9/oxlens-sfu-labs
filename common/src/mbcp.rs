// author: kodeholic (powered by Claude)
//! MBCP Floor Control — RTCP APP packet builder/parser (client-side)
//!
//! 서버 oxlens-sfu-server/src/transport/udp/rtcp.rs 의 MBCP 부분을 클라이언트용으로 포팅.
//! 패킷 포맷은 RFC 3550 Section 6.7 (APP) 준수.

/// RTCP APP payload type
pub const RTCP_PT_APP: u8 = 204;

/// APP subtype: Floor Request (PTT 누름)
pub const SUBTYPE_FREQ: u8 = 0;
/// APP subtype: Floor Release (PTT 뗌)
pub const SUBTYPE_FREL: u8 = 1;
/// APP subtype: Floor Taken (서버 → 클라이언트)
pub const SUBTYPE_FTKN: u8 = 2;
/// APP subtype: Floor Idle (서버 → 클라이언트)
pub const SUBTYPE_FIDL: u8 = 3;
/// APP subtype: Floor Revoke (서버 → 클라이언트)
pub const SUBTYPE_FRVK: u8 = 4;
/// APP subtype: Floor Ping (발화자 생존 확인)
pub const SUBTYPE_FPNG: u8 = 5;

/// APP name: "MBCP"
pub const APP_NAME: [u8; 4] = [b'M', b'B', b'C', b'P'];

/// 파싱된 MBCP 메시지
#[derive(Debug, Clone)]
pub struct MbcpMessage {
    pub subtype: u8,
    pub ssrc: u32,
    pub data: Option<String>,
}

impl MbcpMessage {
    /// 사람이 읽기 좋은 subtype 이름
    pub fn subtype_name(&self) -> &'static str {
        match self.subtype {
            SUBTYPE_FREQ => "FREQ",
            SUBTYPE_FREL => "FREL",
            SUBTYPE_FTKN => "FTKN",
            SUBTYPE_FIDL => "FIDL",
            SUBTYPE_FRVK => "FRVK",
            SUBTYPE_FPNG => "FPNG",
            _ => "UNKNOWN",
        }
    }
}

/// MBCP APP 패킷 빌더
///
/// data가 없으면 12바이트 (header + SSRC + name)
/// data가 있으면 12 + ceil4(data.len()) 바이트
pub fn build(subtype: u8, ssrc: u32, data: Option<&str>) -> Vec<u8> {
    let data_bytes = data.map(|s| s.as_bytes()).unwrap_or(&[]);
    let padded_len = (data_bytes.len() + 3) & !3;
    let total_len = 12 + padded_len;
    let length_field = (total_len / 4) - 1;

    let mut buf = vec![0u8; total_len];

    // V=2, P=0, subtype
    buf[0] = 0x80 | (subtype & 0x1F);
    // PT=204 (APP)
    buf[1] = RTCP_PT_APP;
    // length
    buf[2..4].copy_from_slice(&(length_field as u16).to_be_bytes());
    // SSRC
    buf[4..8].copy_from_slice(&ssrc.to_be_bytes());
    // name = "MBCP"
    buf[8..12].copy_from_slice(&APP_NAME);
    // application-dependent data
    if !data_bytes.is_empty() {
        buf[12..12 + data_bytes.len()].copy_from_slice(data_bytes);
    }

    buf
}

/// RTCP APP 패킷에서 MBCP 메시지 파싱
///
/// RTCP compound 내 단일 패킷(offset 0 기준) 또는 전체 compound 내 첫 MBCP APP 파싱.
pub fn parse(buf: &[u8]) -> Option<MbcpMessage> {
    if buf.len() < 12 {
        return None;
    }

    let pt = buf[1];
    if pt != RTCP_PT_APP {
        return None;
    }

    if &buf[8..12] != &APP_NAME {
        return None;
    }

    let subtype = buf[0] & 0x1F;
    let ssrc = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);

    let data = if buf.len() > 12 {
        let raw = &buf[12..];
        let trimmed = raw.iter()
            .position(|&b| b == 0)
            .map(|pos| &raw[..pos])
            .unwrap_or(raw);
        if trimmed.is_empty() {
            None
        } else {
            String::from_utf8(trimmed.to_vec()).ok()
        }
    } else {
        None
    };

    Some(MbcpMessage { subtype, ssrc, data })
}

/// RTCP compound 패킷에서 모든 MBCP APP 블록 추출
pub fn parse_compound(buf: &[u8]) -> Vec<MbcpMessage> {
    let mut results = Vec::new();
    let mut offset = 0;

    while offset + 4 <= buf.len() {
        let pt = buf[offset + 1];
        let length_words = u16::from_be_bytes([buf[offset + 2], buf[offset + 3]]) as usize;
        let pkt_len = (length_words + 1) * 4;

        if pkt_len == 0 || offset + pkt_len > buf.len() {
            break;
        }

        if pt == RTCP_PT_APP {
            if let Some(msg) = parse(&buf[offset..offset + pkt_len]) {
                results.push(msg);
            }
        }

        offset += pkt_len;
    }

    results
}

/// 편의 빌더: Floor Request
pub fn build_freq(ssrc: u32) -> Vec<u8> {
    build(SUBTYPE_FREQ, ssrc, None)
}

/// 편의 빌더: Floor Release
pub fn build_frel(ssrc: u32) -> Vec<u8> {
    build(SUBTYPE_FREL, ssrc, None)
}

/// 편의 빌더: Floor Ping
pub fn build_fpng(ssrc: u32) -> Vec<u8> {
    build(SUBTYPE_FPNG, ssrc, None)
}

/// subtype 번호 → 이름 (필터링 디버그용)
pub fn subtype_name(subtype: u8) -> &'static str {
    match subtype {
        SUBTYPE_FREQ => "FREQ",
        SUBTYPE_FREL => "FREL",
        SUBTYPE_FTKN => "FTKN",
        SUBTYPE_FIDL => "FIDL",
        SUBTYPE_FRVK => "FRVK",
        SUBTYPE_FPNG => "FPNG",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_freq() {
        let pkt = build_freq(0x12345678);
        assert_eq!(pkt.len(), 12);
        let msg = parse(&pkt).unwrap();
        assert_eq!(msg.subtype, SUBTYPE_FREQ);
        assert_eq!(msg.ssrc, 0x12345678);
        assert!(msg.data.is_none());
    }

    #[test]
    fn test_roundtrip_with_data() {
        let pkt = build(SUBTYPE_FTKN, 0, Some("user_42"));
        assert_eq!(pkt.len(), 20); // 12 + pad4(7) = 12 + 8
        let msg = parse(&pkt).unwrap();
        assert_eq!(msg.subtype, SUBTYPE_FTKN);
        assert_eq!(msg.data.as_deref(), Some("user_42"));
    }

    #[test]
    fn test_parse_compound() {
        let freq = build_freq(0x11);
        let frel = build_frel(0x22);
        let mut compound = freq;
        compound.extend_from_slice(&frel);
        let msgs = parse_compound(&compound);
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].subtype, SUBTYPE_FREQ);
        assert_eq!(msgs[1].subtype, SUBTYPE_FREL);
    }
}
