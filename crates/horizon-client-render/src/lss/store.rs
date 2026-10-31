//! On-disk columns streamed from an LSS server, one dimension at a time.
use crate::fast_hash::FxHashMap;
use crate::lod::Column;
use crate::lod::cache::{self, DiskCache};
use crate::streaming::{LodDetail, LodSampler, LodSource};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

const INDEX_MAGIC: &[u8; 4] = b"HLSS";
const INDEX_FORMAT: u32 = 1;
const INDEX_HEADER: usize = 4 + 4 + 8;
const RECORD_HEADER: usize = 4 + 4 + 8 + 2;

pub struct Store {
    cache: DiskCache,
    coarse: RwLock<FxHashMap<(i32, i32), (i64, Column)>>,
    index: Mutex<File>,
    dir: PathBuf,
}

impl Store {
    pub fn open(dir: PathBuf, key: u64) -> std::io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        let index_path = dir.join("index.bin");
        let (coarse, index_file) = load_index(&dir, &index_path, key)?;
        Ok(Self {
            cache: DiskCache::new(dir.clone(), key),
            coarse: RwLock::new(coarse),
            index: Mutex::new(index_file),
            dir,
        })
    }

    pub fn key(states: usize, biomes: usize) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::hash::DefaultHasher::new();
        "lss".hash(&mut hasher);
        INDEX_FORMAT.hash(&mut hasher);
        states.hash(&mut hasher);
        biomes.hash(&mut hasher);
        hasher.finish()
    }

    pub fn stamps(&self) -> Vec<((i32, i32), i64)> {
        self.coarse
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(&pos, (ts, _))| (pos, *ts))
            .collect()
    }

    pub fn put(&self, pos: (i32, i32), timestamp: i64, columns: &[Column; 256], coarse: Column) {
        self.cache.replace(pos.0, pos.1, columns);
        {
            let mut map = self.coarse.write().unwrap_or_else(|e| e.into_inner());
            map.insert(pos, (timestamp, coarse.clone()));
        }
        let mut encoded = Vec::new();
        cache::encode_column(&mut encoded, &coarse);
        let mut record = Vec::with_capacity(RECORD_HEADER + encoded.len());
        record.extend_from_slice(&pos.0.to_le_bytes());
        record.extend_from_slice(&pos.1.to_le_bytes());
        record.extend_from_slice(&timestamp.to_le_bytes());
        record.extend_from_slice(&(encoded.len() as u16).to_le_bytes());
        record.extend_from_slice(&encoded);
        let Ok(mut file) = self.index.lock() else {
            return;
        };
        if file.write_all(&record).is_ok() {
            let _ = file.sync_all();
        }
        maybe_compact(&self.dir, &mut file, &self.coarse);
    }

    pub fn columns(&self, pos: (i32, i32)) -> Option<Arc<[Column; 256]>> {
        self.cache.get(pos.0, pos.1)
    }

    pub fn coarse(&self, pos: (i32, i32)) -> Option<Column> {
        self.coarse
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&pos)
            .map(|(_, column)| column.clone())
    }

    pub fn len(&self) -> usize {
        self.coarse.read().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl LodSource for Store {
    fn sampler(self: Arc<Self>) -> Box<dyn LodSampler> {
        Box::new(StoreSampler {
            store: self,
            column_cache: VecDeque::new(),
            details: Vec::new(),
        })
    }
}

struct StoreSampler {
    store: Arc<Store>,
    column_cache: VecDeque<((i32, i32), Option<Arc<[Column; 256]>>)>,
    details: Vec<(i32, i32)>,
}

impl LodSampler for StoreSampler {
    fn fork(&self) -> Option<Box<dyn LodSampler>> {
        Some(Box::new(StoreSampler {
            store: self.store.clone(),
            column_cache: VecDeque::new(),
            details: Vec::new(),
        }))
    }

    fn request_detail(&mut self, _chunks: &[(i32, i32)], _center: [i32; 2]) {
        for (pos, _) in self.store.stamps() {
            if !self.details.contains(&pos) {
                self.details.push(pos);
            }
        }
    }

    fn drain_details(&mut self) -> Vec<LodDetail> {
        std::mem::take(&mut self.details)
            .into_iter()
            .filter_map(|pos| self.store.columns(pos).map(|columns| (pos, columns)))
            .collect()
    }

    fn sample(&mut self, x: i32, z: i32) -> Option<crate::lod::Surface> {
        self.reduced_column(x, z, 1).surface
    }

    fn reduced_column(&mut self, x: i32, z: i32, width: i32) -> Column {
        let chunk = (x >> 4, z >> 4);
        if width >= 16 {
            return self.store.coarse(chunk).unwrap_or_default();
        }
        let columns = match self.chunk_columns(chunk) {
            Some(columns) => columns,
            None => return Column::default(),
        };
        Column::reduced(
            columns.as_ref(),
            (x & 15) as usize,
            (z & 15) as usize,
            width as usize,
        )
    }

    fn grid(&mut self, x: i32, z: i32, stride: i32, side: usize) -> Vec<Column> {
        let mut columns = Vec::with_capacity(side * side);
        for dz in 0..side {
            for dx in 0..side {
                columns.push(self.reduced_column(
                    x + dx as i32 * stride,
                    z + dz as i32 * stride,
                    stride,
                ));
            }
        }
        columns
    }
}

impl StoreSampler {
    fn chunk_columns(&mut self, key: (i32, i32)) -> Option<Arc<[Column; 256]>> {
        if let Some(index) = self.column_cache.iter().position(|(at, _)| *at == key) {
            let (_, columns) = self.column_cache.remove(index).unwrap();
            self.column_cache.push_front((key, columns.clone()));
            return columns;
        }
        let columns = self.store.columns(key);
        self.column_cache.push_front((key, columns.clone()));
        self.column_cache.truncate(128);
        columns
    }
}

fn load_index(
    dir: &Path,
    path: &Path,
    key: u64,
) -> std::io::Result<(FxHashMap<(i32, i32), (i64, Column)>, File)> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let size = file.metadata()?.len();
    if size < INDEX_HEADER as u64 {
        write_index_header(&mut file, key)?;
        return Ok((FxHashMap::default(), file));
    }
    let mut header = [0u8; INDEX_HEADER];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header)?;
    if header[..4] != *INDEX_MAGIC
        || header[4..8] != INDEX_FORMAT.to_le_bytes()
        || header[8..] != key.to_le_bytes()
    {
        clear_storage(dir, path)?;
        file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        write_index_header(&mut file, key)?;
        return Ok((FxHashMap::default(), file));
    }
    let mut map = FxHashMap::default();
    let mut at = INDEX_HEADER as u64;
    let mut record_count = 0usize;
    let mut record = vec![0u8; RECORD_HEADER];
    while at + RECORD_HEADER as u64 <= size {
        file.seek(SeekFrom::Start(at))?;
        if file.read_exact(&mut record).is_err() {
            break;
        }
        record_count += 1;
        let x = i32::from_le_bytes(record[0..4].try_into().unwrap());
        let z = i32::from_le_bytes(record[4..8].try_into().unwrap());
        let ts = i64::from_le_bytes(record[8..16].try_into().unwrap());
        let len = u16::from_le_bytes(record[16..18].try_into().unwrap()) as u64;
        if at + RECORD_HEADER as u64 + len > size {
            break;
        }
        let mut payload = vec![0u8; len as usize];
        file.read_exact(&mut payload)?;
        let mut cursor = 0;
        if let Some(column) = cache::decode_column(&payload, &mut cursor)
            && cursor == payload.len()
        {
            map.insert((x, z), (ts, column));
        }
        at += RECORD_HEADER as u64 + len;
    }
    if at < size {
        file.set_len(at)?;
    }
    let live = map.len();
    if record_count > live * 2 + 1024 {
        compact_if_needed(dir, path, key, &mut map, &mut file)?;
    }
    Ok((map, file))
}

fn compact_if_needed(
    dir: &Path,
    path: &Path,
    key: u64,
    map: &mut FxHashMap<(i32, i32), (i64, Column)>,
    file: &mut File,
) -> std::io::Result<()> {
    let size = file.metadata()?.len();
    let record_count = count_index_records(size);
    let live = map.len();
    if record_count <= live * 2 + 1024 {
        return Ok(());
    }
    let tmp = dir.join("index.bin.new");
    let mut out = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    write_index_header(&mut out, key)?;
    for (&(x, z), (ts, column)) in map.iter() {
        let mut encoded = Vec::new();
        cache::encode_column(&mut encoded, column);
        out.write_all(&x.to_le_bytes())?;
        out.write_all(&z.to_le_bytes())?;
        out.write_all(&ts.to_le_bytes())?;
        out.write_all(&(encoded.len() as u16).to_le_bytes())?;
        out.write_all(&encoded)?;
    }
    out.sync_all()?;
    drop(out);
    std::fs::rename(tmp, path)?;
    *file = OpenOptions::new().read(true).write(true).open(path)?;
    Ok(())
}

fn maybe_compact(
    dir: &Path,
    file: &mut File,
    coarse: &RwLock<FxHashMap<(i32, i32), (i64, Column)>>,
) {
    let Ok(size) = file.metadata().map(|m| m.len()) else {
        return;
    };
    let live = coarse.read().unwrap_or_else(|e| e.into_inner()).len();
    let record_count = count_index_records(size);
    if record_count <= live * 2 + 1024 {
        return;
    }
    let path = dir.join("index.bin");
    let key = {
        let mut header = [0u8; INDEX_HEADER];
        if file.seek(SeekFrom::Start(0)).is_err() || file.read_exact(&mut header).is_err() {
            return;
        }
        u64::from_le_bytes(header[8..].try_into().unwrap())
    };
    let mut map = coarse.write().unwrap_or_else(|e| e.into_inner()).clone();
    let _ = compact_if_needed(dir, &path, key, &mut map, file);
}

fn count_index_records(file_size: u64) -> usize {
    if file_size <= INDEX_HEADER as u64 {
        return 0;
    }
    // Lower bound: smallest record is an empty coarse column.
    (file_size - INDEX_HEADER as u64).div_ceil(RECORD_HEADER as u64) as usize
}

fn write_index_header(file: &mut File, key: u64) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    file.write_all(INDEX_MAGIC)?;
    file.write_all(&INDEX_FORMAT.to_le_bytes())?;
    file.write_all(&key.to_le_bytes())?;
    Ok(())
}

fn clear_storage(dir: &Path, index_path: &Path) -> std::io::Result<()> {
    if let Ok(read) = std::fs::read_dir(dir) {
        for entry in read.flatten() {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == "index.bin" || name.ends_with(".hlod") {
                let _ = std::fs::remove_file(path);
            }
        }
    }
    let _ = std::fs::remove_file(index_path);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lod::{Span, Surface};
    use horizon_world::registry::BlockStateId;

    fn temp_dir(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("horizon-lss-store-{tag}-{}", std::process::id()))
    }

    fn span(min: i16, max: i16, state: u32) -> Span {
        Span {
            min,
            max,
            state: BlockStateId(state),
            biome: 0,
            light: 240,
        }
    }

    fn chunk_columns(seed: u32) -> [Column; 256] {
        std::array::from_fn(|i| {
            let i = i as u32 + seed;
            Column {
                surface: Some(Surface {
                    height: 60 + (i % 7) as i32,
                    state: BlockStateId(i),
                    water: None,
                    biome: 0,
                }),
                spans: vec![span(0, 10, i)],
            }
        })
    }

    fn coarse_for(columns: &[Column; 256]) -> Column {
        crate::lod::coarse(columns)
    }

    #[test]
    fn put_round_trip_and_latest_wins() {
        let dir = temp_dir("put");
        let _ = std::fs::remove_dir_all(&dir);
        let store = Store::open(dir.clone(), 1).unwrap();
        let a = chunk_columns(0);
        let b = chunk_columns(1);
        store.put((3, 4), 10, &a, coarse_for(&a));
        assert_eq!(store.len(), 1);
        assert!(store.columns((3, 4)).is_some());
        assert!(store.coarse((3, 4)).is_some());
        store.put((3, 4), 11, &b, coarse_for(&b));
        assert_eq!(*store.columns((3, 4)).unwrap(), b);
        store.put((3, 4), 12, &a, coarse_for(&a));
        assert_eq!(*store.columns((3, 4)).unwrap(), a);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_restores_stamps_and_coarse() {
        let dir = temp_dir("reopen");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let store = Store::open(dir.clone(), 2).unwrap();
            let cols = chunk_columns(5);
            store.put((1, 2), 99, &cols, coarse_for(&cols));
        }
        let store = Store::open(dir.clone(), 2).unwrap();
        assert_eq!(store.stamps(), vec![((1, 2), 99)]);
        assert!(store.coarse((1, 2)).is_some());
        assert!(store.columns((1, 2)).is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn key_mismatch_clears_folder() {
        let dir = temp_dir("key");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let store = Store::open(dir.clone(), 3).unwrap();
            let cols = chunk_columns(0);
            store.put((0, 0), 1, &cols, coarse_for(&cols));
        }
        let store = Store::open(dir.clone(), 4).unwrap();
        assert_eq!(store.len(), 0);
        assert!(store.columns((0, 0)).is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn torn_index_tail_cut() {
        let dir = temp_dir("torn");
        let _ = std::fs::remove_dir_all(&dir);
        {
            let store = Store::open(dir.clone(), 5).unwrap();
            let cols = chunk_columns(0);
            store.put((2, 3), 7, &cols, coarse_for(&cols));
        }
        let path = dir.join("index.bin");
        let size = std::fs::metadata(&path).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0, 1, 2, 3, 4]).unwrap();
        drop(file);
        let store = Store::open(dir.clone(), 5).unwrap();
        assert_eq!(store.stamps(), vec![((2, 3), 7)]);
        assert!(std::fs::metadata(&path).unwrap().len() <= size + 5);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sampler_stride_16_uses_coarse_and_stride_4_detail() {
        let dir = temp_dir("sampler");
        let _ = std::fs::remove_dir_all(&dir);
        let store = Arc::new(Store::open(dir.clone(), 6).unwrap());
        let cols = chunk_columns(0);
        let coarse = coarse_for(&cols);
        store.put((0, 0), 1, &cols, coarse.clone());
        let mut sampler = store.sampler();
        let wide = sampler.reduced_column(0, 0, 16);
        assert_eq!(wide, coarse);
        let fine = sampler.reduced_column(4, 4, 4);
        assert_ne!(fine.spans.len(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_chunk_samples_default() {
        let dir = temp_dir("missing");
        let _ = std::fs::remove_dir_all(&dir);
        let store = Arc::new(Store::open(dir.clone(), 7).unwrap());
        let mut sampler = store.sampler();
        assert_eq!(sampler.reduced_column(0, 0, 1), Column::default());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
