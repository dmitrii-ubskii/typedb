/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */



pub mod bench_rocks_impl;

use std::collections::HashMap;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use rand::random;
use rand_core::RngCore;
use xoshiro::Xoshiro256Plus;
use crate::bench_rocks_impl::rocks_database::{create_typedb, rocks_sync_wal, rocks_with_wal, rocks_without_wal};

const N_DATABASES: usize = 1;
const N_COL_FAMILIES_PER_DB: usize = 1;

const KEY_SIZE: usize = 64;
const VALUE_SIZE: usize = 0;


pub trait RocksDatabase : Sync + Send {
    fn open_batch(&self) -> impl RocksWriteBatch;
}

pub trait RocksWriteBatch {
    type CommitError: std::fmt::Debug;
    fn put(&mut self, database_index: usize, key: [u8; KEY_SIZE]);
    fn commit(self) -> Result<(), Self::CommitError>;
}


pub struct BenchmarkResult {
    pub batch_timings: Vec<Duration>,
    pub total_time: Duration,
}

impl BenchmarkResult {
    fn print_report(&self, runner: &BenchmarkRunner) {
        println!("-- Report for benchmark ---");
        println!("threads = {}, batches={}, batch_size={} ---", runner.n_threads, runner.n_batches, runner.batch_size);
        println!("key-size: {KEY_SIZE}; value_size: {VALUE_SIZE}");
        // println!("Batch timings (ns):");
        // println!("- - - - - - - -");
        // self.batch_timings.iter().enumerate().for_each(|(batch_id, time)| {
        //     println!("{:8}: {:12}", batch_id, time.as_nanos());
        // });
        // println!("- - - - - - - -");

        let n_keys: usize = runner.n_batches * runner.batch_size;
        let data_size_mb : f64 = ((n_keys * (KEY_SIZE + VALUE_SIZE)) as f64) / ((1024 * 1024) as f64) ;
        println!("Summary:");
        println!("Total time: {:12} ns; total_keys: {:10}; data_size: {:8} MB\nrate: {:.2} keys/s = {:.2} MB/s ",
                 self.total_time.as_nanos(), n_keys, data_size_mb,
                 n_keys as f64 / self.total_time.as_secs_f64(), data_size_mb / self.total_time.as_secs_f64(),
        );
    }
}

pub struct BenchmarkRunner {
    n_threads: u16,
    n_batches: usize,
    batch_size: usize,
}

impl BenchmarkRunner {
    const VALUE_EMPTY :[u8;0] = [];
    fn run(&self, database_arc: &impl RocksDatabase) -> BenchmarkResult {
        debug_assert_eq!(1, N_DATABASES, "I've not bothered implementing multiple databases");
        // Pre-generating data is 3x-4x faster.
        let mut pre_rng = Xoshiro256Plus::from_seed_u64(random());
        let mut data: Vec<[u8; KEY_SIZE]> = Vec::new();
        for _ in 0..(self.n_batches * self.batch_size) {
            data.push(Self::generate_key_value(&mut pre_rng).0);
        }

        let batch_timings: Vec<RwLock<Duration>> = (0..self.n_batches).into_iter().map(|_| RwLock::new(Duration::from_secs(0))).collect();
        let batch_counter = AtomicUsize::new(0);
        let benchmark_start_instant = Instant::now();
        thread::scope(|s| {
            for _ in 0..self.n_threads {
                s.spawn(|| {
                    loop {
                        let batch_number = batch_counter.fetch_add(1, Ordering::Relaxed);
                        if batch_number >= self.n_batches { break; }
                        let mut write_batch = database_arc.open_batch();
                        let batch_start_instant = Instant::now();
                        for batch_idx in 0..self.batch_size {
                            let k = data.get(batch_number * self.batch_size + batch_idx).unwrap();
                            write_batch.put(0, k.clone());
                        }
                        write_batch.commit().unwrap();
                        let batch_stop =  batch_start_instant.elapsed();
                        let mut duration_for_batch = batch_timings.get(batch_number).unwrap().write().unwrap();
                        *duration_for_batch = batch_stop;
                    }
                });
            }
        });
        assert!(batch_counter.load(Ordering::Relaxed) >= self.n_batches);
        let total_time = benchmark_start_instant.elapsed();
        BenchmarkResult {
            batch_timings : batch_timings.iter().map(|x| x.read().unwrap().clone()).collect(),
            total_time
        }
    }

    fn generate_key_value(rng: &mut Xoshiro256Plus) -> ([u8; KEY_SIZE], [u8; VALUE_SIZE]) {
        let mut key : [u8; KEY_SIZE] = [0; KEY_SIZE];
        rng.fill_bytes(&mut key);
        (key , Self::VALUE_EMPTY)
    }
}

fn get_arg_as<T: std::str::FromStr>(args:&HashMap<String, String>, key: &str) -> Result<T, String> {
    match args.get(&key.to_string()) {
        None => Err(format!("Pass {key} as arg")),
        Some(value) => value.parse().map_err(|_| format!("Error parsing value for {key}"))
    }
}

fn run_for(args: &HashMap<String, String>, database: &impl RocksDatabase) {
    let benchmarker = BenchmarkRunner {
        n_threads: get_arg_as::<u16>(&args, "threads").unwrap(),
        n_batches: get_arg_as::<usize>(&args, "batches").unwrap(),
        batch_size: get_arg_as::<usize>(&args, "batch_size").unwrap(),
    };


    let report = benchmarker.run(database);
    println!("Done");
    report.print_report(&benchmarker);
}


fn main() {
    let args : HashMap<String, String> = std::env::args()
        .filter_map(|arg| arg.split_once("=").map(|(s1, s2)| (s1.to_string(), s2.to_string())))
        .collect();

    match get_arg_as::<String>(&args, "database").unwrap().as_str() {
        "rocks_no_wal" => run_for(&args, &rocks_without_wal::<N_DATABASES>().unwrap()),
        "rocks_wal" => run_for(&args, &rocks_with_wal::<N_DATABASES>().unwrap()),
        "rocks_sync" => run_for(&args, &rocks_sync_wal::<N_DATABASES>().unwrap()),
        "typedb" => run_for(&args, &create_typedb::<N_DATABASES>().unwrap()),
        _ => panic!("Unrecognised argument for database. Supported: rocks_no_wal, rocks_wal, rocks_sync, typedb")
    }
}
