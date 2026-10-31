//! LSS play-phase payload codecs (protocol 20).
use horizon_protocol::buf::{Reader, Writer};
use std::borrow::Cow;

pub const PROTOCOL_VERSION: i32 = 20;
pub const CAP_VOXEL_COLUMNS: i32 = 1;
pub const CAP_ZSTD_COLUMNS: i32 = 2;
pub const CAP_MAP_POLICY: i32 = 8;
pub const MAX_BATCH_REQUESTS: usize = 1024;
pub const MAX_BATCH_RESPONSES: usize = 4096;
pub const MAX_DIRTY: usize = 10240;
pub const MAX_SECTIONS_SIZE: usize = 2_097_152;
pub const MAX_DIMENSION: usize = 256;
pub const RESPONSE_UP_TO_DATE: i8 = 1;
pub const RESPONSE_NOT_GENERATED: i8 = 2;
pub const CODEC_RAW: u8 = 0;
pub const CODEC_ZSTD: u8 = 1;

pub const HANDSHAKE: &str = "lss:handshake_c2s";
pub const CLIENT_INFO: &str = "lss:client_info";
pub const BATCH_REQUEST: &str = "lss:batch_chunk_req";
pub const SESSION_CONFIG: &str = "lss:session_config";
pub const BATCH_RESPONSE: &str = "lss:batch_response";
pub const VOXEL_COLUMN: &str = "lss:voxel_column";
pub const DIRTY_COLUMNS: &str = "lss:dirty_columns";

const MAX_SECTIONS_DECOMPRESSED: usize = 16 * 1024 * 1024;

fn map_err(e: horizon_protocol::Error) -> String {
    e.to_string()
}

pub fn pack(pos: (i32, i32)) -> i64 {
    (i64::from(pos.0) << 32) | (pos.1 as u32 as i64)
}

pub fn unpack(p: i64) -> (i32, i32) {
    ((p >> 32) as i32, p as i32)
}

pub fn register() -> Vec<u8> {
    let channels = [SESSION_CONFIG, BATCH_RESPONSE, VOXEL_COLUMN, DIRTY_COLUMNS];
    channels.join("\0").into_bytes()
}

pub fn handshake(capabilities: i32) -> Vec<u8> {
    let mut w = Writer::new();
    w.varint(PROTOCOL_VERSION);
    w.varint(capabilities);
    w.data
}

pub fn client_info(data_version: i32) -> Vec<u8> {
    let mut w = Writer::new();
    w.varint(data_version);
    w.data
}

pub fn batch_request(wants: &[((i32, i32), i64)]) -> Vec<u8> {
    let count = wants.len().min(MAX_BATCH_REQUESTS);
    let mut w = Writer::new();
    w.varint(count as i32);
    for &((x, z), ts) in wants.iter().take(count) {
        w.i64(pack((x, z)));
        w.i64(ts);
    }
    w.data
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionConfig {
    pub protocol: i32,
    pub enabled: bool,
    pub lod_distance: i32,
    pub generation: bool,
    pub data_version: Option<i32>,
    pub flags: Option<i32>,
}

pub fn session_config(data: &[u8]) -> Result<SessionConfig, String> {
    let mut r = Reader::new(data);
    let protocol = r.varint().map_err(map_err)?;
    if protocol != PROTOCOL_VERSION {
        return Ok(SessionConfig {
            protocol,
            enabled: false,
            lod_distance: 0,
            generation: false,
            data_version: None,
            flags: None,
        });
    }
    let enabled = r.bool().map_err(map_err)?;
    let lod_distance = r.varint().map_err(map_err)?;
    let generation = r.bool().map_err(map_err)?;
    let data_version = if r.remaining() > 0 {
        Some(r.varint().map_err(map_err)?)
    } else {
        None
    };
    let flags = if r.remaining() > 0 {
        let raw = r.varint().map_err(map_err)?;
        if raw < 0 { None } else { Some(raw) }
    } else {
        None
    };
    let _ = r.rest();
    Ok(SessionConfig {
        protocol,
        enabled,
        lod_distance,
        generation,
        data_version,
        flags,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Response {
    UpToDate,
    NotGenerated,
    Unknown(i8),
}

pub fn batch_response(data: &[u8]) -> Result<Vec<(Response, (i32, i32))>, String> {
    let mut r = Reader::new(data);
    let count = r.varint().map_err(map_err)?;
    if count < 0 || count as usize > MAX_BATCH_RESPONSES {
        return Err(format!("batch response count out of range: {count}"));
    }
    let count = count as usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let ty = r.i8().map_err(map_err)?;
        let packed = r.i64().map_err(map_err)?;
        let response = match ty {
            RESPONSE_UP_TO_DATE => Response::UpToDate,
            RESPONSE_NOT_GENERATED => Response::NotGenerated,
            other => Response::Unknown(other),
        };
        out.push((response, unpack(packed)));
    }
    if r.remaining() != 0 {
        return Err(format!(
            "{} trailing bytes after batch response",
            r.remaining()
        ));
    }
    Ok(out)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VoxelColumn {
    pub pos: (i32, i32),
    pub dimension: String,
    pub timestamp: i64,
    pub source: u8,
    pub codec: u8,
    pub sections: Vec<u8>,
}

pub fn voxel_column(data: &[u8]) -> Result<VoxelColumn, String> {
    let mut r = Reader::new(data);
    let x = r.i32().map_err(map_err)?;
    let z = r.i32().map_err(map_err)?;
    let dimension = r.string(MAX_DIMENSION).map_err(map_err)?;
    let timestamp = r.i64().map_err(map_err)?;
    let source = r.u8().map_err(map_err)?;
    let codec = r.u8().map_err(map_err)?;
    let sections = r.byte_array(MAX_SECTIONS_SIZE).map_err(map_err)?;
    if r.remaining() != 0 {
        return Err(format!(
            "{} trailing bytes after voxel column",
            r.remaining()
        ));
    }
    Ok(VoxelColumn {
        pos: (x, z),
        dimension,
        timestamp,
        source,
        codec,
        sections,
    })
}

pub fn dirty_columns(data: &[u8]) -> Result<Vec<(i32, i32)>, String> {
    let mut r = Reader::new(data);
    let raw = r.varint().map_err(map_err)?;
    if raw < 0 {
        return Ok(Vec::new());
    }
    let keep = (raw as usize).min(MAX_DIRTY);
    let mut out = Vec::with_capacity(keep);
    for _ in 0..keep {
        out.push(unpack(r.i64().map_err(map_err)?));
    }
    if raw as usize > keep {
        let skip = (raw as usize - keep) * 8;
        let skip = skip.min(r.remaining());
        r.bytes(skip).map_err(map_err)?;
    }
    Ok(out)
}

pub fn section_bytes(column: &VoxelColumn) -> Result<Cow<'_, [u8]>, String> {
    match column.codec {
        CODEC_RAW => Ok(Cow::Borrowed(&column.sections)),
        CODEC_ZSTD => {
            use std::io::Read;
            let mut decoder = ruzstd::decoding::StreamingDecoder::new(column.sections.as_slice())
                .map_err(|e| format!("zstd decode: {e}"))?;
            let mut out = Vec::new();
            decoder
                .read_to_end(&mut out)
                .map_err(|e| format!("zstd decode: {e}"))?;
            if out.len() > MAX_SECTIONS_DECOMPRESSED {
                return Err(format!(
                    "decompressed section bytes {} exceed cap {}",
                    out.len(),
                    MAX_SECTIONS_DECOMPRESSED
                ));
            }
            Ok(Cow::Owned(out))
        }
        other => Err(format!("unknown column codec {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use horizon_protocol::buf::Writer;

    #[test]
    fn pack_unpack_round_trip() {
        for (x, z) in [(0, 0), (1, -2), (-3, 4), (i32::MIN, 0), (0, i32::MAX)] {
            assert_eq!(unpack(pack((x, z))), (x, z));
        }
    }

    #[test]
    fn handshake_bytes() {
        assert_eq!(handshake(11), vec![20, 11]);
    }

    #[test]
    fn batch_request_one_entry() {
        let body = batch_request(&[((5, -7), 42)]);
        let mut r = Reader::new(&body);
        assert_eq!(r.varint().unwrap(), 1);
        assert_eq!(unpack(r.i64().unwrap()), (5, -7));
        assert_eq!(r.i64().unwrap(), 42);
        assert_eq!(r.remaining(), 0);
    }

    #[test]
    fn register_payload() {
        let bytes = register();
        let expected =
            format!("{SESSION_CONFIG}\0{BATCH_RESPONSE}\0{VOXEL_COLUMN}\0{DIRTY_COLUMNS}");
        assert_eq!(bytes, expected.as_bytes());
    }

    #[test]
    fn session_config_v20_round_trip() {
        let mut w = Writer::new();
        w.varint(20);
        w.bool(true);
        w.varint(128);
        w.bool(false);
        w.varint(777);
        w.varint(0);
        let cfg = session_config(&w.data).unwrap();
        assert_eq!(
            cfg,
            SessionConfig {
                protocol: 20,
                enabled: true,
                lod_distance: 128,
                generation: false,
                data_version: Some(777),
                flags: Some(0),
            }
        );
    }

    #[test]
    fn session_config_foreign_protocol_disabled() {
        let mut w = Writer::new();
        w.varint(16);
        w.bool(true);
        let cfg = session_config(&w.data).unwrap();
        assert!(!cfg.enabled);
        assert_eq!(cfg.protocol, 16);
    }

    #[test]
    fn batch_response_round_trip() {
        let mut w = Writer::new();
        w.varint(2);
        w.i8(RESPONSE_UP_TO_DATE);
        w.i64(pack((1, 2)));
        w.i8(RESPONSE_NOT_GENERATED);
        w.i64(pack((3, 4)));
        let out = batch_response(&w.data).unwrap();
        assert_eq!(
            out,
            [
                (Response::UpToDate, (1, 2)),
                (Response::NotGenerated, (3, 4)),
            ]
        );
    }

    #[test]
    fn rejects_oversized_batch_response() {
        let mut w = Writer::new();
        w.varint(MAX_BATCH_RESPONSES as i32 + 1);
        assert!(batch_response(&w.data).is_err());
    }

    #[test]
    fn voxel_column_round_trip() {
        let mut w = Writer::new();
        w.i32(10);
        w.i32(-20);
        w.string("minecraft:overworld");
        w.i64(99);
        w.u8(1);
        w.u8(CODEC_RAW);
        w.byte_array(&[1, 2, 3]);
        let col = voxel_column(&w.data).unwrap();
        assert_eq!(col.pos, (10, -20));
        assert_eq!(col.dimension, "minecraft:overworld");
        assert_eq!(col.timestamp, 99);
        assert_eq!(col.sections, vec![1, 2, 3]);
    }

    #[test]
    fn voxel_column_rejects_trailing() {
        let mut w = Writer::new();
        w.i32(0);
        w.i32(0);
        w.string("minecraft:overworld");
        w.i64(0);
        w.u8(0);
        w.u8(0);
        w.byte_array(&[]);
        w.u8(0);
        assert!(voxel_column(&w.data).is_err());
    }

    #[test]
    fn dirty_columns_skips_excess() {
        let mut w = Writer::new();
        w.varint(3);
        w.i64(pack((1, 1)));
        w.i64(pack((2, 2)));
        w.i64(pack((3, 3)));
        assert_eq!(
            dirty_columns(&w.data).unwrap(),
            vec![(1, 1), (2, 2), (3, 3)]
        );
    }

    #[test]
    fn dirty_columns_negative_count_empty() {
        let mut w = Writer::new();
        w.varint(-1);
        assert!(dirty_columns(&w.data).unwrap().is_empty());
    }

    #[test]
    fn zstd_section_round_trip() {
        let raw = b"section-bytes-payload".to_vec();
        let compressed = ruzstd::encoding::compress_to_vec(
            raw.as_slice(),
            ruzstd::encoding::CompressionLevel::Fastest,
        );
        let col = VoxelColumn {
            pos: (0, 0),
            dimension: "minecraft:overworld".into(),
            timestamp: 1,
            source: 0,
            codec: CODEC_ZSTD,
            sections: compressed,
        };
        assert_eq!(section_bytes(&col).unwrap().as_ref(), raw.as_slice());
    }
}
