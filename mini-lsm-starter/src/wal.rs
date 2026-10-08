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

use anyhow::{Context, Result, bail};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};

/// The write-ahead log of a memtable. Each write batch is encoded as
/// `| body_len (u32) | (key_len (u16) | key | ts (u64) | value_len (u16) | value)* | checksum (u32) |`,
/// where the checksum covers the body.
pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from WAL")?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut rbuf: &[u8] = buf.as_slice();
        while rbuf.has_remaining() {
            let batch_size = rbuf.get_u32() as usize;
            if rbuf.remaining() < batch_size + std::mem::size_of::<u32>() {
                bail!("incomplete WAL");
            }
            let mut batch_buf = &rbuf[..batch_size];
            let checksum = crc32fast::hash(batch_buf);
            let mut kv_pairs = Vec::new();
            while batch_buf.has_remaining() {
                let key_len = batch_buf.get_u16() as usize;
                let key = Bytes::copy_from_slice(&batch_buf[..key_len]);
                batch_buf.advance(key_len);
                let key = KeyBytes::from_bytes_with_ts(key, batch_buf.get_u64());
                let value_len = batch_buf.get_u16() as usize;
                let value = Bytes::copy_from_slice(&batch_buf[..value_len]);
                batch_buf.advance(value_len);
                kv_pairs.push((key, value));
            }
            rbuf.advance(batch_size);
            let expected_checksum = rbuf.get_u32();
            if checksum != expected_checksum {
                bail!("WAL checksum mismatch");
            }
            // Only apply the batch once the whole batch has been validated.
            for (key, value) in kv_pairs {
                skiplist.insert(key, value);
            }
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut body = Vec::new();
        for (key, value) in data {
            body.put_u16(key.key_len() as u16);
            body.put_slice(key.key_ref());
            body.put_u64(key.ts());
            body.put_u16(value.len() as u16);
            body.put_slice(value);
        }
        let mut buf = Vec::with_capacity(body.len() + 8);
        buf.put_u32(body.len() as u32);
        buf.put_slice(&body);
        buf.put_u32(crc32fast::hash(&body));
        let mut file = self.file.lock();
        file.write_all(&buf)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
