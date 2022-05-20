// Copyright 2016 Mozilla Foundation
// Copyright 2022 William Brown <william@blackhats.net.au>
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

use crate::cache::{Cache, CacheRead, CacheWrite, Storage};

use std::convert::TryInto;
use std::fs::{create_dir_all, File};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_disk_cache::{ArcDiskCache, CacheObj};
use async_trait::async_trait;

use std::io::{self, BufReader, IoSliceMut, Read, Seek, SeekFrom};

pub struct ConcurrentDiskCache {
    /// A concurrent disk cache based on adaptive replacement caching
    cache: Arc<ArcDiskCache<String, ()>>,
    /// Thread pool to execute disk I/O
    pool: tokio::runtime::Handle,
}

impl ConcurrentDiskCache {
    /// Create a new `DiskCache` rooted at `root`, with `max_size` as the maximum cache size on-disk, in bytes.
    pub fn new<T: AsRef<Path>>(
        root: T,
        max_size: u64,
        durable_fs: bool,
        pool: &tokio::runtime::Handle,
    ) -> Result<ConcurrentDiskCache, anyhow::Error> {
        create_dir_all(root.as_ref())?;

        Ok(ConcurrentDiskCache {
            cache: Arc::new(ArcDiskCache::new(
                max_size.try_into().unwrap(),
                root.as_ref(),
                durable_fs,
            )),
            pool: pool.clone(),
        })
    }
}

pub struct CacheObjReader {
    _cache_obj: CacheObj<String, ()>,
    fh: BufReader<File>,
}

#[async_trait]
impl Storage for ConcurrentDiskCache {
    async fn get(&self, key: &str) -> Result<Cache, anyhow::Error> {
        let key = key.to_string();
        let cache = self.cache.clone();
        self.pool
            .spawn_blocking(move || {
                match cache.get(&key) {
                    Some(cache_obj) => {
                        // The way that arccache works, we need to pin and keep the cache_obj
                        // which keeps the filehandle reference alive even during eviction events
                        // this means that due to how this works, we need a wrapper for cache_obj
                        // that implements our reader.

                        let fh = match File::open(&cache_obj.fhandle.path) {
                            Ok(f) => BufReader::new(f),
                            Err(e) => {
                                error!("Failed to open file handle {:?}", e);
                                return Err(e.into());
                            }
                        };

                        let cache_obj_reader = CacheObjReader {
                            _cache_obj: cache_obj,
                            fh,
                        };

                        let hit = CacheRead::from(cache_obj_reader)?;

                        Ok(Cache::Hit(hit))
                    }
                    None => {
                        trace!("DiskCache::get({}): FileNotInCache", key);
                        return Ok(Cache::Miss);
                    }
                }
            })
            .await?
    }

    async fn put(&self, key: &str, entry: CacheWrite) -> Result<Duration, anyhow::Error> {
        let key = key.to_string();
        let cache = self.cache.clone();
        self.pool
            .spawn_blocking(move || {
                let start = Instant::now();
                // This buffers the content in memory
                let v = entry.finish()?;
                cache.insert_bytes(key, (), v.as_slice());
                Ok(start.elapsed())
            })
            .await?
    }

    fn location(&self) -> String {
        format!("Local disk (concurrent): {:?}", self.cache.path())
    }

    async fn current_size(&self) -> Result<Option<u64>, anyhow::Error> {
        let stats = self.cache.view_stats();
        let used_memory = stats.freq + stats.recent;
        Ok(Some(used_memory))
    }

    async fn max_size(&self) -> Result<Option<u64>, anyhow::Error> {
        let shared_max = self.cache.view_stats().shared_max;
        Ok(Some(shared_max))
    }
}

impl Read for CacheObjReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        self.fh.read(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize, io::Error> {
        self.fh.read_vectored(bufs)
    }

    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize, io::Error> {
        self.fh.read_to_end(buf)
    }

    fn read_to_string(&mut self, buf: &mut String) -> Result<usize, io::Error> {
        self.fh.read_to_string(buf)
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), io::Error> {
        self.fh.read_exact(buf)
    }
}

impl Seek for CacheObjReader {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, io::Error> {
        self.fh.seek(pos)
    }

    fn rewind(&mut self) -> Result<(), io::Error> {
        self.fh.rewind()
    }

    fn stream_position(&mut self) -> Result<u64, io::Error> {
        self.fh.stream_position()
    }
}
