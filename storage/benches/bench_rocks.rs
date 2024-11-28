/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        RwLock,
    },
    thread,
    time::{Duration, Instant},
};
use std::marker::PhantomData;

use itertools::Itertools;
use rand::random;
use rand_core::RngCore;
use xoshiro::Xoshiro256Plus;

use crate::bench_rocks_impl::rocks_database::{create_typedb, create_nontransactional_rocks, create_transactional_rocks};

pub mod bench_rocks_impl;

const N_DATABASES: usize = 1;

const KEY_SIZE: usize = 40;
const VALUE_SIZE: usize = 0;

const N_FORWARDS_PER_SEEK: usize = 2;// TODO: Make arg

pub trait RocksDatabase: Sync + Send {
    fn open_batch(&self) -> impl RocksWriteBatch;
    fn open_read_tx(&self) -> impl RocksReadTransaction;
}

pub trait RocksWriteBatch {
    type CommitError: std::fmt::Debug;
    fn put(&mut self, database_index: usize, key: [u8; KEY_SIZE]);
    fn commit(self) -> Result<(), Self::CommitError>;
}

pub trait RocksReadTransaction {
    fn open_iter_at(&self, database_index: usize, key: &[u8]) -> impl RocksIterator;
}

pub trait RocksIterator {
    type Error;
    fn next(&mut self) -> Option<Result<[u8; KEY_SIZE], Self::Error>>;
}


pub struct BenchmarkResult<Runner> {
    pub batch_timings: Vec<Duration>,
    pub total_time: Duration,
    runner: PhantomData<Runner>
}

impl BenchmarkResult<ReadBenchmarkRunner> {
    pub(crate) fn print_report(&self, args: &CLIArgs, runner: &ReadBenchmarkRunner) {
        println!("-- Report for ReadBenchmark: {} ---", args.database);
        println!("threads = {}, n_txn={}, n_iterators/txn={}, n_forwards={} ---", runner.n_threads, runner.n_transactions, runner.n_seeks_per_transaction, N_FORWARDS_PER_SEEK);
        println!("key-size: {KEY_SIZE}; value_size: {VALUE_SIZE}");
        // println!("cli_args: [{}]", args.for_report());
        // println!("- - - Batch timings (ns): - - -");
        // self.batch_timings.iter().enumerate().for_each(|(batch_id, time)| {
        //     println!("{:8}: {:12}", batch_id, time.as_nanos());
        // });
        let seeks_per_thread = runner.n_transactions * runner.n_seeks_per_transaction;
        println!("Summary:");
        println!(
            "Total time: {:12} ms; total_seeks: {:10}; rate: {:.2} seeks/s ({:.2} per thread)",
            self.total_time.as_secs_f64() * 1000.0,
            args.n_threads as usize * seeks_per_thread,
            seeks_per_thread as f64 * args.n_threads as f64 / self.total_time.as_secs_f64(),
            seeks_per_thread as f64 / self.total_time.as_secs_f64(),
        );
    }
}

impl BenchmarkResult<WriteBenchmarkRunner> {
    fn print_report(&self, args: &CLIArgs, runner: &WriteBenchmarkRunner) {
        println!("-- Report for WriteBenchmark: {} ---", args.database);
        println!("threads = {}, batches={}, batch_size={} ---", runner.n_threads, runner.n_batches, runner.batch_size);
        println!("key-size: {KEY_SIZE}; value_size: {VALUE_SIZE}");
        println!("cli_args: [{}]", args.for_report());
        // println!("- - - Batch timings (ns): - - -");
        // self.batch_timings.iter().enumerate().for_each(|(batch_id, time)| {
        //     println!("{:8}: {:12}", batch_id, time.as_nanos());
        // });
        let n_keys: usize = runner.n_batches * runner.batch_size;
        let data_size_mb: f64 = ((n_keys * (KEY_SIZE + VALUE_SIZE)) as f64) / ((1024 * 1024) as f64);
        println!("Summary:");
        println!(
            "Total time: {:12} ms; total_keys: {:10}; data_size: {:8} MB\nrate: {:.2} keys/s = {:.2} MB/s ",
            self.total_time.as_secs_f64() * 1000.0,
            n_keys,
            data_size_mb,
            n_keys as f64 / self.total_time.as_secs_f64(),
            data_size_mb / self.total_time.as_secs_f64(),
        );
        println!("--- End Report ---\n");
    }
}


fn generate_key_value(rng: &mut Xoshiro256Plus) -> ([u8; KEY_SIZE], [u8; VALUE_SIZE]) {
    const VALUE_EMPTY: [u8; 0] = [];
    // Rust's inbuilt ThreadRng is secure and slow. Xoshiro is significantly faster.
    // This ~(50 GB/s) is faster than generating 64 random bytes (~6 GB/s) or loading pre-generated (~18 GB/s).
    let mut key: [u8; KEY_SIZE] = [0; KEY_SIZE];
    let mut z = rng.next_u64();
    for start in (0..KEY_SIZE).step_by(8) {
        key[start..][..8].copy_from_slice(&z.to_le_bytes());
        z = u64::rotate_left(z, 1); // Rotation beats the compression.
    }
    (key, VALUE_EMPTY)
}

pub struct WriteBenchmarkRunner {
    n_threads: u16,
    n_batches: usize,
    batch_size: usize,
}

impl WriteBenchmarkRunner {
    fn run(&self, database_arc: &impl RocksDatabase) -> BenchmarkResult<Self> {
        debug_assert_eq!(1, N_DATABASES, "I've not bothered implementing multiple databases");
        let batch_timings: Vec<RwLock<Duration>> =
            (0..self.n_batches).map(|_| RwLock::new(Duration::from_secs(0))).collect();
        let batch_counter = AtomicUsize::new(0);
        let benchmark_start_instant = Instant::now();
        thread::scope(|s| {
            for _ in 0..self.n_threads {
                s.spawn(|| {
                    let mut in_rng = Xoshiro256Plus::from_seed_u64(random());
                    loop {
                        let batch_number = batch_counter.fetch_add(1, Ordering::Relaxed);
                        if batch_number >= self.n_batches {
                            break;
                        }
                        let mut write_batch = database_arc.open_batch();
                        let batch_start_instant = Instant::now();
                        for _ in 0..self.batch_size {
                            let (k, _) = generate_key_value(&mut in_rng);
                            write_batch.put(0, k);
                        }
                        write_batch.commit().unwrap();
                        let batch_stop = batch_start_instant.elapsed();
                        let mut duration_for_batch = batch_timings.get(batch_number).unwrap().write().unwrap();
                        *duration_for_batch = batch_stop;
                    }
                });
            }
        });
        assert!(batch_counter.load(Ordering::Relaxed) >= self.n_batches);
        let total_time = benchmark_start_instant.elapsed();
        BenchmarkResult { batch_timings: batch_timings.iter().map(|x| *x.read().unwrap()).collect(), total_time, runner: PhantomData }
    }
}

struct ReadBenchmarkRunner {
    n_threads: u16,
    n_transactions: usize,
    n_seeks_per_transaction: usize,
    n_forwards_per_seek: usize,
}

impl ReadBenchmarkRunner {
    fn run(&self, database: &impl RocksDatabase) -> BenchmarkResult<Self> {
        debug_assert_eq!(1, N_DATABASES, "I've not bothered implementing multiple databases");
        let batch_timings: Vec<RwLock<Duration>> =
            (0..self.n_transactions).map(|_| RwLock::new(Duration::from_secs(0))).collect();
        let batch_counter = AtomicUsize::new(0);
        let benchmark_start_instant = Instant::now();
        thread::scope(|s| {
            for _ in 0..self.n_threads {
                s.spawn(|| {
                    let mut in_rng = Xoshiro256Plus::from_seed_u64(random());
                    loop {
                        let tx_number = batch_counter.fetch_add(1, Ordering::Relaxed);
                        if tx_number >= self.n_transactions {
                            break;
                        }
                        let mut tx = database.open_read_tx();
                        let batch_start_instant = Instant::now();
                        for _ in 0..self.n_seeks_per_transaction {
                            let (k, _) = generate_key_value(&mut in_rng);
                            let mut iter = tx.open_iter_at(0, &k);
                            for _ in 0..self.n_forwards_per_seek {
                                match iter.next() {
                                    None => { break; }
                                    Some(k) => { std::hint::black_box(&k); }
                                }
                            }
                        }
                        let batch_stop = batch_start_instant.elapsed();
                        let mut duration_for_batch = batch_timings.get(tx_number).unwrap().write().unwrap();
                        *duration_for_batch = batch_stop;
                    }
                });
            }
        });
        assert!(batch_counter.load(Ordering::Relaxed) >= self.n_transactions);
        let total_time = benchmark_start_instant.elapsed();
        BenchmarkResult { batch_timings: batch_timings.iter().map(|x| *x.read().unwrap()).collect(), total_time, runner: PhantomData }
    }
}

#[derive(Default)]
struct CLIArgs {
    database: String,

    n_threads: u16,
    n_batches: usize,
    batch_size: usize,

    rocks_disable_wal: Option<bool>,
    rocks_set_sync: Option<bool>,         // Needs WAL, fsync on write.
    rocks_write_buffer_mb: Option<usize>, // Size of memtable per column family. Useful for getting a no-op timing.
}

impl CLIArgs {
    const VALID_ARGS: [&'static str; 7] = [
        "database",
        "threads",
        "batches",
        "batch_size",
        "rocks_disable_wal",
        "rocks_set_sync",
        "rocks_write_buffer_mb",
    ];
    fn get_arg_as<T: std::str::FromStr>(
        args: &HashMap<String, String>,
        key: &str,
        required: bool,
    ) -> Result<Option<T>, String> {
        match args.get(&key.to_owned()) {
            None => {
                if required {
                    Err(format!("Pass {key} as arg"))
                } else {
                    Ok(None)
                }
            }
            Some(value) => Ok(Some(value.parse().map_err(|_| format!("Error parsing value for {key}"))?)),
        }
    }
    fn parse_args() -> Result<CLIArgs, String> {
        let arg_map: HashMap<String, String> = std::env::args()
            .filter_map(|arg| arg.split_once('=').map(|(s1, s2)| (s1.to_string(), s2.to_string())))
            .collect();
        let invalid_keys = arg_map.keys().filter(|key| !Self::VALID_ARGS.contains(&key.as_str())).join(",");
        if !invalid_keys.is_empty() {
            return Err(format!("Invalid keys: {invalid_keys}"));
        }

        let args = CLIArgs {
            database: Self::get_arg_as::<String>(&arg_map, "database", true)?.unwrap(),
            n_threads: Self::get_arg_as::<u16>(&arg_map, "threads", true)?.unwrap(),
            n_batches: Self::get_arg_as::<usize>(&arg_map, "batches", true)?.unwrap(),
            batch_size: Self::get_arg_as::<usize>(&arg_map, "batch_size", true)?.unwrap(),

            rocks_disable_wal: Self::get_arg_as::<bool>(&arg_map, "rocks_disable_wal", false)?,
            rocks_set_sync: Self::get_arg_as::<bool>(&arg_map, "rocks_set_sync", false)?,
            rocks_write_buffer_mb: Self::get_arg_as::<usize>(&arg_map, "rocks_write_buffer_mb", false)?,
        };

        Ok(args)
    }

    fn for_report(&self) -> String {
        let mut s = "".to_string();
        if let Some(val) = self.rocks_disable_wal {
            s.push_str(format!("rocks_disable_wal={val}").as_str());
        }
        if let Some(val) = self.rocks_set_sync {
            s.push_str(format!("rocks_set_sync={val}").as_str());
        }
        if let Some(val) = self.rocks_write_buffer_mb {
            s.push_str(format!("rocks_write_buffer_mb={val}").as_str());
        }
        s
    }
}

fn run_writes_for(args: &CLIArgs, database: &impl RocksDatabase) {
    let benchmarker =
        WriteBenchmarkRunner { n_threads: args.n_threads, n_batches: args.n_batches, batch_size: args.batch_size };
    let report = benchmarker.run(database);
    report.print_report(args, &benchmarker);
}

fn run_reads_for(args: &CLIArgs, database: &impl RocksDatabase) {
    let benchmarker = // TODO: Expand args
        ReadBenchmarkRunner { n_threads: args.n_threads, n_transactions: args.n_batches, n_seeks_per_transaction: args.batch_size , n_forwards_per_seek: N_FORWARDS_PER_SEEK};
    let report = benchmarker.run(database);
    report.print_report(args, &benchmarker);
}

fn run_for(args: &CLIArgs, database: &impl RocksDatabase) {
    run_writes_for(args, database);
    run_reads_for(args, database);
}

fn main() {
    let args = CLIArgs::parse_args().unwrap();
    match args.database.as_str() {
        "rocks" => run_for(&args, &create_nontransactional_rocks::<N_DATABASES>(&args).unwrap()),
        "typedb" => run_for(&args, &create_typedb::<N_DATABASES>().unwrap()),
        "txn_rocks" => run_for(&args, &create_transactional_rocks::<N_DATABASES>(&args).unwrap()),
        _ => panic!("Unrecognised argument for database. Supported: rocks, typedb"),
    }
}
