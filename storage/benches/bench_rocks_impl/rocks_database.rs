/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use non_transactional_rocks::NonTransactionalRocks;
use rocksdb::{Options, WriteOptions};
use storage::StorageOpenError;

use crate::{bench_rocks_impl::rocks_database::typedb_database::TypeDBDatabase, CLIArgs};
use crate::bench_rocks_impl::rocks_database::transactional_rocks::TransactionalRocks;

fn database_options(args: &CLIArgs) -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    if let Some(write_buffer_size_mb) = args.rocks_write_buffer_mb {
        opts.set_write_buffer_size(write_buffer_size_mb * 1024 * 1024);
    }
    opts
}

fn write_options(args: &CLIArgs) -> WriteOptions {
    let mut write_options = WriteOptions::default();
    if let Some(disable_wal) = args.rocks_disable_wal {
        write_options.disable_wal(disable_wal);
    }
    if let Some(set_sync) = args.rocks_set_sync {
        write_options.set_sync(set_sync);
    }
    write_options
}

pub fn create_nontransactional_rocks<const N_DATABASES: usize>(args: &CLIArgs) -> Result<NonTransactionalRocks<N_DATABASES>, rocksdb::Error> {
    NonTransactionalRocks::<N_DATABASES>::setup(database_options(args), write_options(args))
}

pub fn create_transactional_rocks<const N_DATABASES: usize>(args: &CLIArgs) -> Result<TransactionalRocks<N_DATABASES>, rocksdb::Error> {
    TransactionalRocks::<N_DATABASES>::setup(database_options(args), write_options(args))
}


pub fn create_typedb<const N_DATABASES: usize>() -> Result<TypeDBDatabase<N_DATABASES>, StorageOpenError> {
    TypeDBDatabase::<N_DATABASES>::setup()
}

mod non_transactional_rocks {
    use std::iter::zip;

    use rocksdb::{Options, WriteBatch, WriteOptions, DB, IteratorMode, Direction};
    use test_utils::{create_tmp_dir, TempDir};

    use crate::{KEY_SIZE, RocksDatabase, RocksIterator, RocksReadTransaction, RocksWriteBatch};

    pub struct NonTransactionalRocks<const N_DATABASES: usize> {
        databases: [DB; crate::N_DATABASES],
        write_options: WriteOptions,
        _path: TempDir,
    }

    impl<const N_DATABASES: usize> NonTransactionalRocks<N_DATABASES> {
        pub(super) fn setup(options: Options, write_options: WriteOptions) -> Result<Self, rocksdb::Error> {
            let path = create_tmp_dir();
            let databases = std::array::from_fn(|i| DB::open(&options, path.join(format!("db_{i}"))).unwrap());

            Ok(Self { _path: path, databases, write_options })
        }
    }

    impl<const N_DATABASES: usize> RocksDatabase for NonTransactionalRocks<N_DATABASES> {
        fn open_batch(&self) -> impl RocksWriteBatch {
            let write_batches = std::array::from_fn(|_| WriteBatch::default());
            NonTransactionalWriteBatch { database: self, write_batches }
        }

        fn open_read_tx(&self) -> impl RocksReadTransaction {
            NonTransactionalTransaction{ database: self }
        }
    }

    pub struct NonTransactionalWriteBatch<'this, const N_DATABASES: usize> {
        database: &'this NonTransactionalRocks<N_DATABASES>,
        write_batches: [WriteBatch; N_DATABASES],
    }

    impl<'this, const N_DATABASES: usize> RocksWriteBatch for NonTransactionalWriteBatch<'this, N_DATABASES> {
        type CommitError = rocksdb::Error;
        fn put(&mut self, database_index: usize, key: [u8; crate::KEY_SIZE]) {
            self.write_batches[database_index].put(key, [])
        }

        fn commit(self) -> Result<(), rocksdb::Error> {
            let write_options = &self.database.write_options;
            for (db, write_batch) in zip(&self.database.databases, self.write_batches) {
                db.write_opt(write_batch, write_options)?
            }
            Ok(())
        }
    }

    pub struct NonTransactionalTransaction<'this, const N_DATABASES: usize> {
        database: &'this NonTransactionalRocks<N_DATABASES>,
    }

    impl<'this, const N_DATABASES: usize> RocksReadTransaction for  NonTransactionalTransaction<'this, N_DATABASES> {
        fn open_iter_at(&self, database_index: usize, key: &[u8]) -> NonTransactionalIterator {
            let mut raw_iterator = self.database.databases[database_index].raw_iterator();
            raw_iterator.seek(&key);
            raw_iterator
        }
    }

    pub type NonTransactionalIterator<'db> = rocksdb::DBRawIterator<'db>;
    impl<'db> RocksIterator for NonTransactionalIterator<'db> {
        type Error = rocksdb::Error;

        fn next(&mut self) -> Option<Result<[u8; KEY_SIZE], Self::Error>> {
            let mut return_bytes = [0; 40];
            if self.valid() {
                return_bytes.copy_from_slice(self.item().unwrap().0);
                Some(Ok(return_bytes))
            } else if self.status().is_err() {
                Some(Err(self.status().err().unwrap().clone()))
            } else {
                None
            }
        }
    }
}

mod typedb_database {
    use std::sync::Arc;

    use bytes::byte_array::ByteArray;
    use durability::wal::WAL;
    use storage::{
        durability_client::WALClient,
        key_value::StorageKeyArray,
        keyspace::{KeyspaceId, KeyspaceSet},
        snapshot::{CommittableSnapshot, SnapshotError, WritableSnapshot, WriteSnapshot},
        MVCCStorage, StorageOpenError,
    };
    use test_utils::{create_tmp_dir, TempDir};

    use crate::{RocksDatabase, RocksWriteBatch, KEY_SIZE, RocksReadTransaction, RocksIterator};

    pub struct TypeDBDatabase<const N_DATABASES: usize> {
        storage: Arc<MVCCStorage<WALClient>>,
        pub path: TempDir,
    }

    impl<const N_DATABASES: usize> TypeDBDatabase<N_DATABASES> {
        pub(super) fn setup() -> Result<Self, StorageOpenError> {
            let name = "bench_rocks__typedb";
            let path = create_tmp_dir();
            let wal = WAL::create(&path).unwrap();
            let storage =
                Arc::new(MVCCStorage::<WALClient>::create::<BenchKeySpace>(name, &path, WALClient::new(wal))?);
            Ok(Self { path, storage })
        }
    }

    pub struct TodoThings;
    impl<const N_DATABASES: usize> RocksDatabase for TypeDBDatabase<N_DATABASES> {
        fn open_batch(&self) -> impl RocksWriteBatch {
            TypeDBSnapshot { snapshot: self.storage.clone().open_snapshot_write() }
        }

        fn open_read_tx(&self) -> TodoThings {
            todo!()
        }
    }

    pub struct TypeDBSnapshot {
        snapshot: WriteSnapshot<WALClient>,
    }

    impl TypeDBSnapshot {
        const KEYSPACES: [BenchKeySpace; 1] = [BenchKeySpace { id: 0 }];
        const KEYSPACE_NAMES: [&'static str; 1] = ["BenchKeySpace[1]"];
    }
    impl RocksWriteBatch for TypeDBSnapshot {
        type CommitError = SnapshotError;
        fn put(&mut self, database_index: usize, key: [u8; KEY_SIZE]) {
            debug_assert_eq!(0, database_index, "Not implemented for multiple databases");
            self.snapshot.put(StorageKeyArray::new(Self::KEYSPACES[0], ByteArray::inline(key, KEY_SIZE)))
        }

        fn commit(self) -> Result<(), Self::CommitError> {
            self.snapshot.commit()?;
            Ok(())
        }
    }

    #[derive(Copy, Clone)]
    pub struct BenchKeySpace {
        id: u8,
    }

    impl KeyspaceSet for BenchKeySpace {
        fn iter() -> impl Iterator<Item = Self> {
            TypeDBSnapshot::KEYSPACES.into_iter()
        }

        fn id(&self) -> KeyspaceId {
            KeyspaceId(self.id)
        }

        fn name(&self) -> &'static str {
            TypeDBSnapshot::KEYSPACE_NAMES[self.id as usize]
        }
    }

    impl RocksReadTransaction for TodoThings {
        fn open_iter_at(&self, database_index: usize, key: &[u8]) -> TodoThings {
            todo!()
        }
    }

    impl RocksIterator for TodoThings {
        type Error = ();

        fn next(&mut self) -> Option<Result<[u8; KEY_SIZE], Self::Error>> {
            todo!()
        }
    }
}

mod transactional_rocks {
    use std::iter::zip;

    use rocksdb::{Options, WriteBatch, WriteOptions, Transaction, OptimisticTransactionDB, WriteBatchWithTransaction, IteratorMode, Direction, DBIteratorWithThreadMode};
    use test_utils::{create_tmp_dir, TempDir};

    use crate::{KEY_SIZE, N_DATABASES, RocksDatabase, RocksIterator, RocksReadTransaction, RocksWriteBatch};

    pub struct TransactionalRocks<const N_DATABASES: usize> {
        databases: [OptimisticTransactionDB; crate::N_DATABASES],
        write_options: WriteOptions,
        _path: TempDir,
    }

    impl<const N_DATABASES: usize> TransactionalRocks<N_DATABASES> {
        pub(super) fn setup(options: Options, write_options: WriteOptions) -> Result<Self, rocksdb::Error> {
            let path = create_tmp_dir();
            let databases = std::array::from_fn(|i| OptimisticTransactionDB::open(&options, path.join(format!("db_{i}"))).unwrap());

            Ok(Self { _path: path, databases, write_options })
        }
    }

    impl<const N_DATABASES: usize> RocksDatabase for TransactionalRocks<N_DATABASES> {
        fn open_batch(&self) -> impl RocksWriteBatch {
            let write_batches = std::array::from_fn(|i|{
                let tx = self.databases[i].transaction();
                let wb = tx.get_writebatch();
                (tx, wb)
            });
            TransactionalWriteBatch { database: self, write_batches }
        }

        fn open_read_tx(&self) -> TransactionalRocksTransaction<'_, N_DATABASES> {
            TransactionalRocksTransaction {
                transactions:std::array::from_fn(|i| self.databases[i].transaction())
            }
        }
    }

    pub struct TransactionalWriteBatch<'this, const N_DATABASES: usize> {
        database: &'this TransactionalRocks<N_DATABASES>,
        write_batches: [(Transaction<'this, OptimisticTransactionDB>, WriteBatchWithTransaction<true>); N_DATABASES],
    }

    impl<'this, const N_DATABASES: usize> RocksWriteBatch for TransactionalWriteBatch<'this, N_DATABASES> {
        type CommitError = rocksdb::Error;
        fn put(&mut self, database_index: usize, key: [u8; crate::KEY_SIZE]) {
            self.write_batches[database_index].1.put(key, [])
        }

        fn commit(self) -> Result<(), rocksdb::Error> {
            let write_options = &self.database.write_options;
            for (db, (tx, write_batch) ) in zip(&self.database.databases, self.write_batches) {
                db.write_opt(write_batch, write_options)?;
                tx.commit()?;
            }
            Ok(())
        }
    }

    pub struct TransactionalRocksTransaction<'db, const N_DATABASES:usize> {
        transactions: [rocksdb::Transaction<'db, OptimisticTransactionDB>; N_DATABASES],
    }

    pub type TransactionalIterator<'db> = DBIteratorWithThreadMode<'db, rocksdb::Transaction<'db, OptimisticTransactionDB>>;
    impl<'db, const N_DATABASES: usize> RocksReadTransaction for TransactionalRocksTransaction<'db, N_DATABASES> {
        fn open_iter_at(&self, database_index: usize, key: &[u8]) -> TransactionalIterator {
            self.transactions[database_index].iterator(IteratorMode::From(key, Direction::Forward))
        }
    }

    impl<'db> RocksIterator for TransactionalIterator<'db> {
        type Error = rocksdb::Error;

        fn next(&mut self) -> Option<Result<[u8; KEY_SIZE], rocksdb::Error>> {
            let mut return_bytes = [0; 40];
            match <Self as Iterator>::next(self) {
                None => None,
                Some(Ok((kbox, _))) => {
                    return_bytes.copy_from_slice(kbox.as_ref());
                    Some(Ok(return_bytes))
                },
                Some(Err(err)) => Some(Err(err)),
            }
        }
    }
}
