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

use std::collections::Bound;
use std::sync::Arc;

use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState, MiniLsm};
use crate::table::SsTableIterator;

impl LsmStorageInner {
    pub fn dump_structure(&self) {
        let snapshot = self.state.read();
        if !snapshot.l0_sstables.is_empty() {
            println!(
                "L0 ({}): {:?}",
                snapshot.l0_sstables.len(),
                snapshot.l0_sstables,
            );
        }
        for (level, files) in &snapshot.levels {
            println!("L{level} ({}): {:?}", files.len(), files);
        }
    }

    pub fn full_dump(&self, filter_key: Option<&str>) -> String {
        fn dump_sst_ids(
            sst_ids: &[usize],
            snapshot: &LsmStorageState,
            s: &mut String,
            filter_key: Option<&str>,
        ) {
            for sst_id in sst_ids {
                let sst = &snapshot.sstables[sst_id];
                *s += &format!("[{}]\n", sst.sst_id());
                let mut iter = SsTableIterator::create_and_seek_to_first(Arc::clone(sst)).unwrap();
                let mut values = Vec::new();
                while iter.is_valid() {
                    let key = iter.key();
                    let key = key.to_key_vec().into_inner();
                    let key = String::from_utf8(key).unwrap();
                    let print = filter_key.map(|k| k == key).unwrap_or(true);
                    if print {
                        values.push(format!(
                            "({}, {})",
                            key,
                            String::from_utf8(iter.value().to_vec()).unwrap()
                        ));
                    }
                    iter.next().unwrap();
                }
                *s += &format!("[{}]\n\n", values.join(","));
            }
        }

        let mut s = "\nFULL DUMP START\n".to_string();

        let snapshot = self.state.read();

        {
            s += &format!("MEM TABLE [{}]\n", snapshot.memtable.id());
            let mut iter = snapshot.memtable.scan(Bound::Unbounded, Bound::Unbounded);
            let mut values = Vec::new();
            while iter.is_valid() {
                let key = iter.key();
                let key = key.to_key_vec().into_inner();
                let key = String::from_utf8(key).unwrap();
                let print = filter_key.map(|k| k == key).unwrap_or(true);
                if print {
                    values.push(format!(
                        "({}, {})",
                        key,
                        String::from_utf8(iter.value().to_vec()).unwrap()
                    ));
                }
                iter.next().unwrap();
            }
            s += &format!("[{}]\n", values.join(","));
        }

        {
            s += "IMM TABLES\n";
            for imm_table in &snapshot.imm_memtables {
                s += &format!("[{}]\n", imm_table.id());
                let mut iter = imm_table.scan(Bound::Unbounded, Bound::Unbounded);
                let mut values = Vec::new();
                while iter.is_valid() {
                    let key = iter.key();
                    let key = key.to_key_vec().into_inner();
                    let key = String::from_utf8(key).unwrap();
                    let print = filter_key.map(|k| k == key).unwrap_or(true);
                    if print {
                        values.push(format!(
                            "({}, {})",
                            key,
                            String::from_utf8(iter.value().to_vec()).unwrap()
                        ));
                    }
                    iter.next().unwrap();
                }
                s += &format!("[{}]\n", values.join(","));
            }
        }

        {
            s += "L0\n";
            dump_sst_ids(&snapshot.l0_sstables, &snapshot, &mut s, filter_key);
        }

        {
            s += "LEVELS\n";
            for (level, sst_ids) in &snapshot.levels {
                s += &format!("[LEVEL {}]\n", level);
                dump_sst_ids(sst_ids, &snapshot, &mut s, filter_key);
            }
        }
        s += "FULL DUMP END\n";

        s
    }
}

impl MiniLsm {
    pub fn dump_structure(&self) {
        self.inner.dump_structure()
    }

    pub fn full_dump(&self, filter_key: Option<&str>) -> String {
        self.inner.full_dump(filter_key)
    }
}
