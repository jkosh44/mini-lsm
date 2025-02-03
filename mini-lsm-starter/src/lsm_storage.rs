#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }

    fn sst_ids(&self) -> impl Iterator<Item = &usize> {
        self.l0_sstables
            .iter()
            .chain(self.levels.iter().flat_map(|(_, sst_ids)| sst_ids.iter()))
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(())?;
        self.compaction_notifier.send(())?;
        self.flush_thread.lock().take().unwrap().join().unwrap();
        self.compaction_thread
            .lock()
            .take()
            .unwrap()
            .join()
            .unwrap();
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        fn transform_tombstone(value: Bytes) -> Option<Bytes> {
            if value.is_empty() {
                None
            } else {
                Some(value)
            }
        }

        let state = Arc::clone(&self.state.read());

        let mem_value = std::iter::once(&state.memtable)
            .chain(state.imm_memtables.iter())
            .filter_map(|memtable| memtable.get(key))
            .next();

        let lower = Bound::Included(key);
        let mut sst_iters = Vec::with_capacity(state.sstables.len());
        for sst_id in state.sst_ids() {
            let table = state.sstables.get(sst_id).expect("known to exist");
            let table = Arc::clone(table);
            match lower {
                Bound::Included(lower) => {
                    let iter = SsTableIterator::create_and_seek_to_key(
                        table,
                        KeySlice::from_slice(lower),
                    )?;
                    sst_iters.push(Box::new(iter));
                }
                Bound::Excluded(lower) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        table,
                        KeySlice::from_slice(lower),
                    )?;
                    if iter.is_valid() && iter.key() == KeySlice::from_slice(lower) {
                        iter.next()?;
                    }
                    sst_iters.push(Box::new(iter));
                }
                Bound::Unbounded => {
                    let iter = SsTableIterator::create_and_seek_to_first(table)?;
                    sst_iters.push(Box::new(iter));
                }
            }
        }
        let sst_iters = MergeIterator::create(sst_iters);

        let value = mem_value
            .or_else(|| {
                if sst_iters.is_valid() && sst_iters.key() == KeySlice::from_slice(key) {
                    Some(Bytes::copy_from_slice(sst_iters.value()))
                } else {
                    None
                }
            })
            .map(|value| if value.is_empty() { None } else { Some(value) });

        Ok(value.flatten())
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let approximate_size = {
            let guard = self.state.read();
            guard.memtable.put(key, value)?;

            guard.memtable.approximate_size()
        };

        if approximate_size >= self.options.target_sst_size {
            let state_lock_observer = self.state_lock.lock();
            if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
                self.force_freeze_memtable(&state_lock_observer)?;
            }
        }

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.put(key, &[])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let mut memtable = Arc::new(MemTable::create(self.next_sst_id()));
        let mut state_guard = self.state.write();
        let mut state = state_guard.as_ref().clone();
        std::mem::swap(&mut memtable, &mut state.memtable);
        state.imm_memtables.insert(0, memtable);
        *state_guard = Arc::new(state);

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();

        let memtable = {
            let state_guard = self.state.read();
            let Some(memtable) = state_guard.imm_memtables.last() else {
                return Ok(());
            };
            memtable.clone()
        };

        let mut builder = SsTableBuilder::new(self.options.block_size);
        memtable.flush(&mut builder)?;
        let sst = builder.build(
            memtable.id(),
            Some(self.block_cache.clone()),
            self.path_of_sst(memtable.id()),
        )?;

        {
            let mut state_guard = self.state.write();
            let mut state = state_guard.as_ref().clone();
            let last = state.imm_memtables.pop().expect("non-empty");
            assert_eq!(memtable.id(), last.id());
            state.l0_sstables.insert(0, sst.sst_id());
            state.sstables.insert(sst.sst_id(), Arc::new(sst));
            *state_guard = Arc::new(state);
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let state = Arc::clone(&self.state.read());

        let memtable_scan = state.memtable.scan(lower, upper);
        let imm_memtables_scan = state
            .imm_memtables
            .iter()
            .map(|imm_memtable| imm_memtable.scan(lower, upper));
        let mem_iters: Vec<_> = std::iter::once(memtable_scan)
            .chain(imm_memtables_scan)
            .map(Box::new)
            .collect();
        let mem_iters = MergeIterator::create(mem_iters);

        let mut sst_iters = Vec::with_capacity(state.sstables.len());
        for sst_id in state.sst_ids() {
            let table = state.sstables.get(sst_id).expect("known to exist");
            let table = Arc::clone(table);
            match upper {
                Bound::Included(upper) => {
                    if KeySlice::from_slice(upper) < table.first_key().as_key_slice() {
                        continue;
                    }
                }
                Bound::Excluded(upper) => {
                    if KeySlice::from_slice(upper) <= table.first_key().as_key_slice() {
                        continue;
                    }
                }
                Bound::Unbounded => {}
            }
            match lower {
                Bound::Included(lower)
                    if KeySlice::from_slice(lower) > table.last_key().as_key_slice() =>
                {
                    continue;
                }
                Bound::Included(lower) => {
                    let iter = SsTableIterator::create_and_seek_to_key(
                        table,
                        KeySlice::from_slice(lower),
                    )?;
                    sst_iters.push(Box::new(iter));
                }
                Bound::Excluded(lower)
                    if KeySlice::from_slice(lower) >= table.last_key().as_key_slice() =>
                {
                    continue;
                }
                Bound::Excluded(lower) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        table,
                        KeySlice::from_slice(lower),
                    )?;
                    if iter.is_valid() && iter.key() == KeySlice::from_slice(lower) {
                        iter.next()?;
                    }
                    sst_iters.push(Box::new(iter));
                }
                Bound::Unbounded => {
                    let iter = SsTableIterator::create_and_seek_to_first(table)?;
                    sst_iters.push(Box::new(iter));
                }
            }
        }
        let sst_iters = MergeIterator::create(sst_iters);

        let lsm_iter_inner = TwoMergeIterator::create(mem_iters, sst_iters)?;
        let upper = upper.map(|upper| Bytes::copy_from_slice(upper));
        let lsm_iter = LsmIterator::new(lsm_iter_inner, upper)?;
        let fused_iter = FusedIterator::new(lsm_iter);
        Ok(fused_iter)
    }
}
