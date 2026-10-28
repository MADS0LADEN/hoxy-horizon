//! Native LSS (LOD Server Support) client for remote Java servers.
//!
//! Speaks the MIT VoX/lod-server-support wire (`lss:*` custom payloads) and
//! injects decoded columns into [`crate::lod::Lod`]. This is how Hoxy receives
//! Voxy-server-side / LSS terrain — not the Horizon-only `horizon:mod_net` path.
//!
//! Protocol details: Hoxy `docs/lss-wire-contract.md`.
use crate::lod::{self, Column, Lod};
use horizon_protocol::buf::{Reader, Writer};
use std::collections::VecDeque;
use std::sync::Arc;

pub const PROTOCOL_VERSION: i32 = 20;
pub const CAP_VOXEL_COLUMNS: i32 = 1;
pub const CAP_ZSTD_COLUMNS: i32 = 2;
pub const CAP_MAP_POLICY: i32 = 8;

pub const CH_HANDSHAKE: &str = "lss:handshake_c2s";
pub const CH_CLIENT_INFO: &str = "lss:client_info";
pub const CH_CHUNK_REQUEST: &str = "lss:batch_chunk_req";
pub const CH_SESSION_CONFIG: &str = "lss:session_config";
pub const CH_BATCH_RESPONSE: &str = "lss:batch_response";
pub const CH_VOXEL_COLUMN: &str = "lss:voxel_column";
pub const CH_DIRTY_COLUMNS: &str = "lss:dirty_columns";

const MAX_BATCH_REQ: usize = 1024;
const MAX_SECTIONS_SIZE: usize = 2_097_152;

/// Pack chunk X/Z the way LSS `PositionUtil` does.
pub fn pack_pos(cx: i32, cz: i32) -> i64 {
    ((cx as i64) << 32) | (cz as u32 as i64)
}

pub fn unpack_x(packed: i64) -> i32 {
    (packed >> 32) as i32
}

pub fn unpack_z(packed: i64) -> i32 {
    packed as i32
}

/// Handshake + optional client_info payloads (configuration or play).
pub fn encode_handshake(data_version: i32) -> Vec<(String, Vec<u8>)> {
    let caps = CAP_VOXEL_COLUMNS | CAP_MAP_POLICY; // zstd later when decoder supports it
    let mut hs = Writer::new();
    hs.varint(PROTOCOL_VERSION);
    hs.varint(caps);
    let mut info = Writer::new();
    info.varint(data_version);
    vec![
        (CH_HANDSHAKE.into(), hs.data),
        (CH_CLIENT_INFO.into(), info.data),
    ]
}

/// Batch chunk request: count, then (packed, clientTimestamp) pairs.
pub fn encode_batch_request(chunks: &[(i32, i32, i64)]) -> Vec<u8> {
    let n = chunks.len().min(MAX_BATCH_REQ);
    let mut w = Writer::new();
    w.varint(n as i32);
    for &(cx, cz, ts) in chunks.iter().take(n) {
        w.i64(pack_pos(cx, cz));
        w.i64(ts);
    }
    w.data
}

#[derive(Clone, Debug)]
pub struct SessionConfig {
    pub protocol: i32,
    pub enabled: bool,
    pub lod_distance_chunks: i32,
    pub generation_enabled: bool,
    pub server_data_version: Option<i32>,
    pub session_flags: Option<i32>,
}

pub fn decode_session_config(data: &[u8]) -> Result<SessionConfig, String> {
    let mut r = Reader::new(data);
    let protocol = r.varint().map_err(|e| e.to_string())?;
    let enabled = r.bool().map_err(|e| e.to_string())?;
    let lod_distance_chunks = r.varint().map_err(|e| e.to_string())?;
    let generation_enabled = r.bool().map_err(|e| e.to_string())?;
    let mut server_data_version = None;
    let mut session_flags = None;
    if protocol == PROTOCOL_VERSION && r.remaining() > 0 {
        server_data_version = Some(r.varint().map_err(|e| e.to_string())?);
        if r.remaining() > 0 {
            session_flags = Some(r.varint().map_err(|e| e.to_string())?);
        }
    }
    Ok(SessionConfig {
        protocol,
        enabled,
        lod_distance_chunks,
        generation_enabled,
        server_data_version,
        session_flags,
    })
}

#[derive(Clone, Debug)]
pub struct VoxelColumnFrame {
    pub chunk_x: i32,
    pub chunk_z: i32,
    pub dimension: String,
    pub timestamp: i64,
    pub source: u8,
    pub codec: u8,
    pub section_bytes: Vec<u8>,
}

/// Current (proto 19+) column: ints, UTF dim, long ts, source u8, codec u8, byte array.
pub fn decode_voxel_column(data: &[u8]) -> Result<VoxelColumnFrame, String> {
    let mut r = Reader::new(data);
    let chunk_x = r.i32().map_err(|e| e.to_string())?;
    let chunk_z = r.i32().map_err(|e| e.to_string())?;
    let dimension = r.string(256).map_err(|e| e.to_string())?;
    let timestamp = r.i64().map_err(|e| e.to_string())?;
    let source = r.u8().map_err(|e| e.to_string())?;
    let codec = r.u8().map_err(|e| e.to_string())?;
    let len = r.varint().map_err(|e| e.to_string())?;
    if !(0..=MAX_SECTIONS_SIZE as i32).contains(&len) {
        return Err(format!("lss column sections len {len}"));
    }
    let section_bytes = r.bytes(len as usize).map_err(|e| e.to_string())?.to_vec();
    if r.remaining() > 0 {
        return Err("lss column trailing bytes".into());
    }
    Ok(VoxelColumnFrame {
        chunk_x,
        chunk_z,
        dimension,
        timestamp,
        source,
        codec,
        section_bytes,
    })
}

/// Runtime state for one remote session.
#[derive(Default)]
pub struct LssClient {
    pub enabled: bool,
    pub session: Option<SessionConfig>,
    pub handshaken: bool,
    /// Pending outbound custom payloads (channel, data).
    pub out: VecDeque<(String, Vec<u8>)>,
    /// Chunks we still want (cx, cz).
    pub want: VecDeque<(i32, i32)>,
    pub ring: i32,
    pub received: u64,
}

impl LssClient {
    pub fn reset(&mut self) {
        self.session = None;
        self.handshaken = false;
        self.out.clear();
        self.want.clear();
        self.received = 0;
    }

    /// Start LSS after play login when the feature is on.
    pub fn on_joined_world(&mut self, data_version: i32) {
        if !self.enabled {
            return;
        }
        self.reset();
        self.enabled = true;
        for p in encode_handshake(data_version) {
            self.out.push_back(p);
        }
        self.handshaken = true;
        self.ring = 64;
    }

    pub fn on_payload(&mut self, channel: &str, data: &[u8]) -> Result<Option<VoxelColumnFrame>, String> {
        match channel {
            CH_SESSION_CONFIG => {
                let cfg = decode_session_config(data)?;
                if cfg.enabled {
                    self.ring = cfg.lod_distance_chunks.clamp(8, 1024);
                }
                self.session = Some(cfg);
                Ok(None)
            }
            CH_VOXEL_COLUMN => Ok(Some(decode_voxel_column(data)?)),
            CH_BATCH_RESPONSE | CH_DIRTY_COLUMNS => Ok(None),
            _ => Ok(None),
        }
    }

    /// Queue a small ring of chunk requests around the player (one batch).
    pub fn request_ring(&mut self, pcx: i32, pcz: i32) {
        if !self.enabled || self.session.as_ref().is_none_or(|s| !s.enabled) {
            return;
        }
        let r = self.ring.min(64);
        let mut batch = Vec::new();
        // Coarse cross + diamond outline for v1.
        for d in (8..=r).step_by(8) {
            for (dx, dz) in [(d, 0), (-d, 0), (0, d), (0, -d), (d, d), (d, -d), (-d, d), (-d, -d)] {
                batch.push((pcx + dx, pcz + dz, 0_i64));
            }
        }
        if !batch.is_empty() {
            self.out
                .push_back((CH_CHUNK_REQUEST.into(), encode_batch_request(&batch)));
        }
    }
}

/// Convert LSS section bytes into Horizon LOD columns.
///
/// **v1 stub:** returns empty columns until the WireSectionCursor port lands.
/// Callers still inject so the pipeline is wired; terrain fill comes next.
pub fn sections_to_lod_columns(_section_bytes: &[u8], _codec: u8) -> Result<[Column; 256], String> {
    if _codec != 0 {
        return Err("lss: zstd columns not decoded yet".into());
    }
    // Placeholder: empty air columns (valid blob / inject shape).
    Ok(std::array::from_fn(|_| Column::default()))
}

/// Inject a decoded LSS column into the far-terrain lod.
pub fn ingest_frame(lod: &mut Lod, frame: &VoxelColumnFrame) -> Result<(), String> {
    let columns = sections_to_lod_columns(&frame.section_bytes, frame.codec)?;
    lod.inject_chunk_columns((frame.chunk_x, frame.chunk_z), Arc::new(columns));
    let _ = lod::encode_lod_columns_blob; // keep encode linked for tests / future
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_round_trip() {
        assert_eq!(unpack_x(pack_pos(-3, 9)), -3);
        assert_eq!(unpack_z(pack_pos(-3, 9)), 9);
    }

    #[test]
    fn handshake_encodes_two_varints() {
        let pkgs = encode_handshake(4189);
        assert_eq!(pkgs[0].0, CH_HANDSHAKE);
        let mut r = Reader::new(&pkgs[0].1);
        assert_eq!(r.varint().unwrap(), PROTOCOL_VERSION);
        assert!(r.varint().unwrap() & CAP_VOXEL_COLUMNS != 0);
    }
}
