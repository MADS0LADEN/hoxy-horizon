//! Mod-scoped binary payloads between a mod's client half and server half
//! (`horizon:mod_net`, docs/mods.md planned `net` capability).
use crate::{
    Error, Result,
    buf::{Reader, Writer},
};

/// The custom payload channel, both ways.
pub const CHANNEL: &str = "horizon:mod_net";
/// Characters in a mod id at most.
pub const MAX_MOD_ID: usize = 64;
/// Bytes of a custom payload's data at most
/// (ServerboundCustomPayloadPacket.MAX_PAYLOAD_SIZE).
pub const MAX_PAYLOAD: usize = 32767;

/// One frame as the server or client reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub mod_id: String,
    pub payload: Vec<u8>,
}

/// `mod_id` and `payload` as one serverbound or clientbound payload.
pub fn encode(mod_id: &str, payload: &[u8]) -> Vec<u8> {
    let mod_id: String = mod_id.chars().take(MAX_MOD_ID).collect();
    let payload = if payload.len() > MAX_PAYLOAD {
        &payload[..MAX_PAYLOAD]
    } else {
        payload
    };
    let mut w = Writer::new();
    w.string(&mod_id);
    w.varint(payload.len() as i32);
    w.bytes(payload);
    w.data
}

/// A payload's frame; an error for one that breaks a cap.
pub fn decode(data: &[u8]) -> Result<Frame> {
    if data.len() > MAX_PAYLOAD {
        return Err(Error::new("mod net: payload too big"));
    }
    let mut r = Reader::new(data);
    let mod_id = r.string(MAX_MOD_ID)?;
    let len = r.varint()?;
    if !(0..=MAX_PAYLOAD as i32).contains(&len) {
        return Err(Error::new(format!("mod net: {len} bytes")));
    }
    let payload = r.bytes(len as usize)?.to_vec();
    if r.remaining() > 0 {
        return Err(Error::new("mod net: trailing bytes"));
    }
    Ok(Frame { mod_id, payload })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frame_round_trips() {
        let payload = vec![1, 2, 3, 4];
        let data = encode("hoxy", &payload);
        assert_eq!(
            decode(&data).unwrap(),
            Frame {
                mod_id: "hoxy".into(),
                payload
            }
        );
    }

    #[test]
    fn oversized_payloads_are_refused() {
        let mut w = Writer::new();
        w.string("m");
        w.varint(MAX_PAYLOAD as i32 + 1);
        assert!(decode(&w.data).is_err());
    }
}
