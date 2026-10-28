//! Native LSS (LOD Server Support) client for remote Java servers.
//!
//! Speaks the MIT VoX/lod-server-support wire (`lss:*` custom payloads) and
//! injects decoded columns into [`crate::lod::Lod`]. This is how Hoxy receives
//! Voxy-server-side / LSS terrain — not the Horizon-only `horizon:mod_net` path.
//!
//! Protocol details: Hoxy `docs/lss-wire-contract.md`.
mod wire_section;

use crate::{
    biome::Biomes,
    chunk::{self, Chunk, Light, Section, MIN_Y, SECTIONS},
    blocks::BlockFacts,
    lod::{self, block_state_kinds, lod_columns_from_chunk_kinds, Column, Lod},
};
use horizon_protocol::buf::{Reader, Writer};
use horizon_world::registry::{BlockRegistry, BlockStateId};
use std::collections::VecDeque;
use std::sync::Arc;
use wire_section::{Layout, WireColumn};

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

/// Registry + biome context for turning LSS identities into Horizon ids.
pub struct LssIngestContext<'a> {
    pub registry: &'static BlockRegistry,
    pub biomes: &'a Biomes,
    pub facts: &'a BlockFacts,
    /// Session protocol from `lss:session_config` (selects section body layout).
    pub protocol: i32,
}

fn section_body_layout(protocol: i32) -> Layout {
    if protocol >= PROTOCOL_VERSION {
        Layout::V20
    } else {
        Layout::Native
    }
}

/// Convert LSS section bytes into Horizon LOD columns (RAW codec only for now).
pub fn sections_to_lod_columns(
    section_bytes: &[u8],
    codec: u8,
    ctx: &LssIngestContext<'_>,
) -> Result<[Column; 256], String> {
    if codec != 0 {
        return Err("lss: zstd columns not decoded yet".into());
    }
    let layout = section_body_layout(ctx.protocol);
    let wire = wire_section::parse(section_bytes, layout)?;
    let kinds = block_state_kinds(ctx.facts);
    let chunk = wire_column_to_chunk(&wire, layout, ctx)?;
    let arc = lod_columns_from_chunk_kinds(&chunk, &kinds);
    let mut out = std::array::from_fn(|_| Column::default());
    out.clone_from_slice(arc.as_ref());
    Ok(out)
}

fn wire_column_to_chunk(
    column: &WireColumn,
    layout: Layout,
    ctx: &LssIngestContext<'_>,
) -> Result<Chunk, String> {
    let mut chunk = Chunk::empty(0, 0);
    let min_section_y = MIN_Y / 16;
    let stone = terminal_block(ctx);

    for section in &column.sections {
        let section_idx = i32::from(section.section_y) - min_section_y;
        if !(0..SECTIONS as i32).contains(&section_idx) {
            continue;
        }
        let si = section_idx as usize;
        let blocks = wire_section::resolve_container(
            &section.blocks,
            wire_section::BLOCK_ENTRIES,
            layout,
            &column.dictionary,
            |entry| resolve_block_palette_entry(entry, layout, &column.dictionary, ctx, stone),
        )?;
        let biomes = wire_section::resolve_container(
            &section.biomes,
            wire_section::BIOME_ENTRIES,
            layout,
            &column.dictionary,
            |entry| resolve_biome_palette_entry(entry, layout, &column.dictionary, ctx),
        )?;
        let mut values = [0u32; 4096];
        for (i, &v) in blocks.iter().enumerate() {
            values[i] = v;
        }
        let mut section_data = Section {
            blocks: Section::from_blocks(&values),
            biomes: {
                let mut biome_vals = [0u32; 64];
                for (i, &b) in biomes.iter().enumerate() {
                    biome_vals[i] = b;
                }
                horizon_core::paletted::PalettedContainer::from_values(chunk::BIOME_STRATEGY, &biome_vals)
            },
            light: merge_section_light(section.block_light.as_deref(), section.sky_light.as_deref()),
        };
        if let Light::Full(boxed) = &mut section_data.light {
            // Keep sky at full bright above terrain when the server omitted sky light
            // (no-sky dimensions still carry block light only).
            if section.sky_light.is_none() {
                for byte in boxed.iter_mut() {
                    *byte = (*byte & 0x0f) | 0xf0;
                }
            }
        }
        chunk.sections[si] = Arc::new(section_data);
    }
    Ok(chunk)
}

fn merge_section_light(block: Option<&[u8]>, sky: Option<&[u8]>) -> Light {
    if block.is_none() && sky.is_none() {
        return Light::Uniform(0xF0);
    }
    let mut out = Box::new([0xF0u8; 4096]);
    for i in 0..4096 {
        let block_n = block.map(|layer| nibble_at(layer, i)).unwrap_or(0);
        let sky_n = sky.map(|layer| nibble_at(layer, i)).unwrap_or(0xF);
        out[i] = u8::from(sky_n << 4) | block_n;
    }
    Light::Full(out)
}

fn nibble_at(layer: &[u8], index: usize) -> u8 {
    let byte = layer[index >> 1];
    if index & 1 == 0 {
        byte & 0x0f
    } else {
        byte >> 4
    }
}

fn terminal_block(ctx: &LssIngestContext<'_>) -> BlockStateId {
    ctx.registry
        .default_state("minecraft:stone")
        .unwrap_or(ctx.facts.air)
}

fn resolve_block_palette_entry(
    entry: i32,
    layout: Layout,
    dictionary: &[String],
    ctx: &LssIngestContext<'_>,
    stone: BlockStateId,
) -> u32 {
    let id = match layout {
        Layout::Native => BlockStateId(entry as u32),
        Layout::V20 => {
            let identity = dictionary
                .get(entry as usize)
                .map(String::as_str)
                .unwrap_or("");
            resolve_block_identity(ctx.registry, identity).unwrap_or(stone)
        }
    };
    id.0
}

fn resolve_biome_palette_entry(
    entry: i32,
    layout: Layout,
    dictionary: &[String],
    ctx: &LssIngestContext<'_>,
) -> u32 {
    match layout {
        Layout::Native => entry as u32,
        Layout::V20 => {
            let identity = dictionary
                .get(entry as usize)
                .map(String::as_str)
                .unwrap_or("minecraft:plains");
            let name = identity.split('[').next().unwrap_or(identity);
            u32::from(ctx.biomes.id(name))
        }
    }
}

/// LSS identity → block state (unknown properties skipped; unknown blocks → stone).
pub fn resolve_block_identity(
    registry: &BlockRegistry,
    identity: &str,
) -> Result<BlockStateId, String> {
    let (name, props) = match identity.split_once('[') {
        Some((n, rest)) => (n, rest.trim_end_matches(']')),
        None => (identity, ""),
    };
    let name = if name.contains(':') {
        name.to_owned()
    } else {
        format!("minecraft:{name}")
    };
    let default = registry
        .default_state(&name)
        .ok_or_else(|| format!("unknown block {name}"))?;
    let mut state = registry
        .state(default)
        .ok_or("missing default state")?
        .clone();
    for pair in props.split(',').filter(|p| !p.is_empty()) {
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        if state.properties.contains_key(k) {
            state.properties.insert(k.to_owned(), v.to_owned());
        }
    }
    registry.resolve(&state).map_err(|e| e.to_string())
}

/// Inject a decoded LSS column into the far-terrain lod.
pub fn ingest_frame(
    lod: &mut Lod,
    frame: &VoxelColumnFrame,
    ctx: &LssIngestContext<'_>,
) -> Result<(), String> {
    let columns = sections_to_lod_columns(&frame.section_bytes, frame.codec, ctx)?;
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

    #[test]
    fn native_corpus_parses_when_present() {
        let path = std::path::Path::new("/tmp/tmp.8eLtLyYaw1/lss/fabric/src/test/resources/nbt-corpus/multi-section.bin");
        if !path.exists() {
            return;
        }
        let body = std::fs::read(path).expect("corpus");
        let column = wire_section::parse(&body, wire_section::Layout::Native).expect("parse");
        assert!(!column.sections.is_empty());
    }
}
