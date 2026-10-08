// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Result, bail};
use bytes::Bytes;
use crossbeam_skiplist::{SkipMap, map::Entry};
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
    mem_table::map_bound,
    mvcc::CommittedTxnData,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    fn check_not_committed(&self) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("cannot operate on committed txn!");
        }
    }

    fn add_to_read_set(&self, key: &[u8]) {
        if let Some(key_hashes) = &self.key_hashes {
            key_hashes.lock().1.insert(farmhash::hash32(key));
        }
    }

    fn add_to_write_set(&self, key: &[u8]) {
        if let Some(key_hashes) = &self.key_hashes {
            key_hashes.lock().0.insert(farmhash::hash32(key));
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.check_not_committed();
        self.add_to_read_set(key);
        // Uncommitted local writes take precedence over the snapshot.
        if let Some(entry) = self.local_storage.get(key) {
            let value = entry.value();
            return Ok((!value.is_empty()).then(|| value.clone()));
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.check_not_committed();
        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |map| map.range((map_bound(lower), map_bound(upper))),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        local_iter.next()?;
        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(
                local_iter,
                self.inner.scan_with_ts(lower, upper, self.read_ts)?,
            )?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.check_not_committed();
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        self.add_to_write_set(key);
    }

    pub fn delete(&self, key: &[u8]) {
        self.check_not_committed();
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
        self.add_to_write_set(key);
    }

    pub fn commit(&self) -> Result<()> {
        self.committed
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .expect("cannot operate on committed txn!");
        let mvcc = self.inner.mvcc();
        // Validation and write must be atomic with respect to other committing transactions.
        let _commit_lock = mvcc.commit_lock.lock();

        if let Some(key_hashes) = &self.key_hashes {
            let key_hashes = key_hashes.lock();
            let (write_set, read_set) = &*key_hashes;
            // Read-only transactions are always serializable.
            if !write_set.is_empty() {
                let committed_txns = mvcc.committed_txns.lock();
                for (_, txn_data) in committed_txns.range((self.read_ts + 1)..) {
                    if read_set
                        .iter()
                        .any(|key_hash| txn_data.key_hashes.contains(key_hash))
                    {
                        bail!("serializable check failed");
                    }
                }
            }
        }

        let batch = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect::<Vec<_>>();
        let commit_ts = self.inner.write_batch_inner(&batch)?;

        if let Some(key_hashes) = &self.key_hashes {
            let mut committed_txns = mvcc.committed_txns.lock();
            let mut key_hashes = key_hashes.lock();
            let (write_set, _) = &mut *key_hashes;
            let old_data = committed_txns.insert(
                commit_ts,
                CommittedTxnData {
                    key_hashes: std::mem::take(write_set),
                    read_ts: self.read_ts,
                    commit_ts,
                },
            );
            assert!(old_data.is_none());

            // Transactions committed at or below the watermark can never conflict with any
            // active or future transaction.
            let watermark = mvcc.watermark();
            while let Some(entry) = committed_txns.first_entry() {
                if *entry.key() <= watermark {
                    entry.remove();
                } else {
                    break;
                }
            }
        }

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts)
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn entry_to_item(entry: Option<Entry<'_, Bytes, Bytes>>) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0[..]
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut iter = Self { _txn: txn, iter };
        iter.skip_deletes()?;
        if iter.is_valid() {
            iter._txn.add_to_read_set(iter.key());
        }
        Ok(iter)
    }

    /// Local deletes are stored as empty values; skip them.
    fn skip_deletes(&mut self) -> Result<()> {
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        self.skip_deletes()?;
        if self.is_valid() {
            self._txn.add_to_read_set(self.key());
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
