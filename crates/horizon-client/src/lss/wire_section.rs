//! LSS `WireSectionCursor` port (NATIVE + V20 section-array bodies).
use horizon_protocol::buf::Reader;

pub const BLOCK_ENTRIES: usize = 4096;
pub const BIOME_ENTRIES: usize = 64;
const NATIVE_BLOCK_PALETTE_MAX_BITS: i32 = 8;
const NATIVE_BIOME_PALETTE_MAX_BITS: i32 = 3;
const BLOCK_MIN_INDEXED_BITS: i32 = 4;
const V20_BLOCK_MAX_BITS: i32 = 12;
const V20_BIOME_MAX_BITS: i32 = 6;
const LIGHT_BYTES: usize = 2048;
const MAX_IDENTITY_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Layout {
    Native,
    V20,
}

#[derive(Clone, Debug)]
pub struct WireContainer {
    pub bits: u8,
    /// `None` = DIRECT (native only): data words hold registry ids.
    pub palette: Option<Vec<i32>>,
    pub data: Vec<i64>,
}

impl WireContainer {
    pub fn is_direct(&self) -> bool {
        self.palette.is_none()
    }
}

#[derive(Clone, Debug)]
pub struct WireSection {
    pub section_y: i8,
    #[allow(dead_code)]
    pub non_empty: i32,
    #[allow(dead_code)]
    pub fluid: i32,
    pub blocks: WireContainer,
    pub biomes: WireContainer,
    pub block_light: Option<Vec<u8>>,
    pub sky_light: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct WireColumn {
    pub dictionary: Vec<String>,
    pub sections: Vec<WireSection>,
}

pub fn parse(body: &[u8], layout: Layout) -> Result<WireColumn, String> {
    let mut r = Reader::new(body);
    let column = read_column(&mut r, layout)?;
    if r.remaining() > 0 {
        return Err(format!("{} trailing bytes after section array", r.remaining()));
    }
    Ok(column)
}

fn read_column(r: &mut Reader<'_>, layout: Layout) -> Result<WireColumn, String> {
    let dictionary = if layout == Layout::V20 {
        let dict_count = read_varint_count(r, "dictCount")?;
        if dict_count > r.remaining() {
            return Err(format!("dictCount {dict_count} exceeds remaining bytes"));
        }
        let mut dict = Vec::with_capacity(dict_count.min(256));
        for _ in 0..dict_count {
            dict.push(read_utf(r)?);
        }
        dict
    } else {
        Vec::new()
    };
    let section_count = read_varint_count(r, "sectionCount")?;
    if layout == Layout::V20 && (section_count == 0) != dictionary.is_empty() {
        return Err(format!(
            "clear-column invariant violated: dictCount={} sectionCount={section_count}",
            dictionary.len()
        ));
    }
    if section_count > r.remaining() {
        return Err(format!(
            "sectionCount {section_count} exceeds remaining bytes"
        ));
    }
    let dict_size = dictionary.len();
    let mut sections = Vec::with_capacity(section_count.min(64));
    for _ in 0..section_count {
        let section_y = r.i8().map_err(|e| e.to_string())?;
        let non_empty = r.i16().map_err(|e| e.to_string())? as i32;
        let fluid = if layout == Layout::V20 {
            r.i16().map_err(|e| e.to_string())? as i32
        } else {
            r.i16().map_err(|e| e.to_string())? as i32
        };
        let blocks = read_container(r, layout, BLOCK_ENTRIES, true, dict_size)?;
        let biomes = read_container(r, layout, BIOME_ENTRIES, false, dict_size)?;
        let block_light = read_light(r)?;
        let sky_light = read_light(r)?;
        sections.push(WireSection {
            section_y,
            non_empty,
            fluid,
            blocks,
            biomes,
            block_light,
            sky_light,
        });
    }
    Ok(WireColumn {
        dictionary,
        sections,
    })
}

fn read_light(r: &mut Reader<'_>) -> Result<Option<Vec<u8>>, String> {
    let present = r.bool().map_err(|e| e.to_string())?;
    if !present {
        return Ok(None);
    }
    Ok(Some(
        r.bytes(LIGHT_BYTES)
            .map_err(|e| e.to_string())?
            .to_vec(),
    ))
}

fn read_container(
    r: &mut Reader<'_>,
    layout: Layout,
    entries: usize,
    is_blocks: bool,
    dict_size: usize,
) -> Result<WireContainer, String> {
    let bits = r.u8().map_err(|e| e.to_string())? as i32;
    check_width(bits, layout, is_blocks)?;
    if bits == 0 {
        let value = read_palette_value(r, layout, is_blocks, dict_size)?;
        return Ok(WireContainer {
            bits: 0,
            palette: Some(vec![value]),
            data: Vec::new(),
        });
    }
    let palette_threshold = if is_blocks {
        NATIVE_BLOCK_PALETTE_MAX_BITS
    } else {
        NATIVE_BIOME_PALETTE_MAX_BITS
    };
    let palette = if layout == Layout::V20 || bits <= palette_threshold {
        let count = read_varint_count(r, "paletteCount")?;
        if count == 0 || count > entries {
            return Err(format!("palette count {count} outside [1, {entries}]"));
        }
        let mut palette = Vec::with_capacity(count);
        for _ in 0..count {
            palette.push(read_palette_value(r, layout, is_blocks, dict_size)?);
        }
        Some(palette)
    } else {
        None
    };
    let values_per_long = 64 / bits;
    let long_count = entries.div_ceil(values_per_long as usize);
    let mut data = Vec::with_capacity(long_count);
    for _ in 0..long_count {
        data.push(r.i64().map_err(|e| e.to_string())?);
    }
    Ok(WireContainer {
        bits: bits as u8,
        palette,
        data,
    })
}

fn read_palette_value(
    r: &mut Reader<'_>,
    layout: Layout,
    is_blocks: bool,
    dict_size: usize,
) -> Result<i32, String> {
    let v = read_varint_count(r, if is_blocks { "block palette" } else { "biome palette" })?;
    if layout == Layout::V20 && v >= dict_size {
        return Err(format!(
            "palette index {v} outside dictionary of {dict_size}"
        ));
    }
    Ok(v as i32)
}

fn check_width(bits: i32, layout: Layout, is_blocks: bool) -> Result<(), String> {
    let max_bits = if layout == Layout::V20 {
        if is_blocks {
            V20_BLOCK_MAX_BITS
        } else {
            V20_BIOME_MAX_BITS
        }
    } else {
        31
    };
    if bits > max_bits {
        return Err(format!(
            "{} container width {bits} exceeds {:?} max {max_bits}",
            if is_blocks { "block" } else { "biome" },
            layout,
        ));
    }
    if is_blocks && (1..BLOCK_MIN_INDEXED_BITS).contains(&bits) {
        return Err(format!(
            "block container width {bits} has no vanilla shape (indexed blocks are 4-8)"
        ));
    }
    Ok(())
}

fn read_varint_count(r: &mut Reader<'_>, what: &str) -> Result<usize, String> {
    let v = r.varint().map_err(|e| e.to_string())?;
    if v < 0 {
        return Err(format!("negative {what} count"));
    }
    Ok(v as usize)
}

fn read_utf(r: &mut Reader<'_>) -> Result<String, String> {
    let len = read_varint_count(r, "utfLen")?;
    if len > MAX_IDENTITY_BYTES {
        return Err(format!("identity length {len} exceeds {MAX_IDENTITY_BYTES}"));
    }
    let bytes = r.bytes(len).map_err(|e| e.to_string())?;
    String::from_utf8(bytes.to_vec()).map_err(|e| e.to_string())
}

/// Unpack `entries` values at `bits` width (vanilla `SimpleBitStorage`).
pub fn unpack(data: &[i64], bits: u8, entries: usize) -> Result<Vec<i32>, String> {
    let bits = i32::from(bits);
    if bits <= 0 || bits > 32 {
        return Err(format!("unpack width {bits}"));
    }
    let values_per_long = 64 / bits;
    let expected = entries.div_ceil(values_per_long as usize);
    if data.len() != expected {
        return Err(format!(
            "unpack over {} longs, expected {expected}",
            data.len()
        ));
    }
    let mask = (1i64 << bits) - 1;
    let mut out = vec![0; entries];
    for (i, slot) in out.iter_mut().enumerate() {
        let word = data[i / values_per_long as usize];
        let shift = (i % values_per_long as usize) * bits as usize;
        *slot = ((word >> shift) & mask) as i32;
    }
    Ok(out)
}

/// Resolve a container to `entries` global registry ids (blocks or biomes).
pub fn resolve_container(
    container: &WireContainer,
    entries: usize,
    layout: Layout,
    _dictionary: &[String],
    mut map_palette: impl FnMut(i32) -> u32,
) -> Result<Vec<u32>, String> {
    if container.bits == 0 {
        let id = container
            .palette
            .as_ref()
            .and_then(|p| p.first())
            .copied()
            .ok_or("malformed single-value container")?;
        let global = map_palette(id);
        return Ok(vec![global; entries]);
    }
    if container.is_direct() {
        if layout == Layout::V20 {
            return Err("v20 has no DIRECT mode".into());
        }
        let indices = unpack(&container.data, container.bits, entries)?;
        return Ok(indices.into_iter().map(|v| v as u32).collect());
    }
    let palette = container.palette.as_ref().ok_or("missing palette")?;
    let indices = unpack(&container.data, container.bits, entries)?;
    Ok(indices
        .into_iter()
        .map(|idx| {
            let entry = palette
                .get(idx as usize)
                .copied()
                .ok_or_else(|| format!("palette index {idx} out of range"))?;
            Ok(map_palette(entry))
        })
        .collect::<Result<Vec<_>, String>>()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unpack_round_trip_width_4() {
        let values: Vec<i32> = (0..4096).map(|i| i % 3).collect();
        let data = pack_test(&values, 4);
        let out = unpack(&data, 4, 4096).unwrap();
        assert_eq!(out, values);
    }

    fn pack_test(values: &[i32], bits: u8) -> Vec<i64> {
        let bits = i32::from(bits);
        let values_per_long = 64 / bits;
        let mut out = vec![0i64; values.len().div_ceil(values_per_long as usize)];
        for (i, &v) in values.iter().enumerate() {
            out[i / values_per_long as usize] |= (v as i64) << ((i % values_per_long as usize) * bits as usize);
        }
        out
    }
}
