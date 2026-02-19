/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    thread,
    thread::JoinHandle,
    time::Instant,
};

use database::{
    database_manager::DatabaseManager,
    query::{execute_schema_query, execute_write_query_in_write},
    transaction::{TransactionRead, TransactionSchema, TransactionWrite},
    Database,
};
use executor::{pipeline::stage::StageIterator, ExecutionInterrupt};
use options::{QueryOptions, TransactionOptions};
use storage::durability_client::WALClient;
use test_utils::{create_tmp_dir, TempDir};

const TOTAL_OPS: usize = 100_000;
const READ_OPS: usize = 10_000;

const DB_NAME: &str = "bench-concurrent-writes";

const SCHEMA: &str = r#"define
    attribute name value string;
    attribute age value integer;
    attribute score value double;
    entity person owns name, owns age, owns score @card(0..);
    relation friendship relates friend @card(0..);
    person plays friendship:friend;
"#;

struct PhaseTimings {
    open_nanos: AtomicU64,
    execute_nanos: AtomicU64,
    commit_nanos: AtomicU64,
    tx_count: AtomicU64,
}

impl PhaseTimings {
    fn new() -> Self {
        Self {
            open_nanos: AtomicU64::new(0),
            execute_nanos: AtomicU64::new(0),
            commit_nanos: AtomicU64::new(0),
            tx_count: AtomicU64::new(0),
        }
    }

    fn record(&self, open_ns: u64, execute_ns: u64, commit_ns: u64) {
        self.open_nanos.fetch_add(open_ns, Ordering::Relaxed);
        self.execute_nanos.fetch_add(execute_ns, Ordering::Relaxed);
        self.commit_nanos.fetch_add(commit_ns, Ordering::Relaxed);
        self.tx_count.fetch_add(1, Ordering::Relaxed);
    }

    fn summary(&self) -> (f64, f64, f64, u64) {
        let count = self.tx_count.load(Ordering::Relaxed);
        if count == 0 {
            return (0.0, 0.0, 0.0, 0);
        }
        let open_avg = self.open_nanos.load(Ordering::Relaxed) as f64 / count as f64 / 1000.0;
        let exec_avg = self.execute_nanos.load(Ordering::Relaxed) as f64 / count as f64 / 1000.0;
        let commit_avg = self.commit_nanos.load(Ordering::Relaxed) as f64 / count as f64 / 1000.0;
        (open_avg, exec_avg, commit_avg, count)
    }
}

// --- Database setup helpers ---

fn create_database(schema: &str) -> (TempDir, Arc<Database<WALClient>>) {
    let tmp_dir = create_tmp_dir();
    let dbm = DatabaseManager::new(&tmp_dir).unwrap();
    dbm.put_database(DB_NAME).unwrap();
    let database = dbm.database(DB_NAME).unwrap();

    let schema_query = typeql::parse_query(schema).unwrap().into_structure().into_schema();
    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let (tx, result) = execute_schema_query(tx, schema_query, schema.to_string());
    result.unwrap();
    tx.commit().1.unwrap();

    (tmp_dir, database)
}

fn seed_persons(database: &Arc<Database<WALClient>>, count: usize) {
    let batch_size = 1000;
    let mut offset = 0;
    while offset < count {
        let n = std::cmp::min(batch_size, count - offset);
        let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
        for i in 0..n {
            let id = offset + i;
            let age: u32 = (id % 100) as u32;
            let query_str = format!(r#"insert $p isa person, has name "person_{id}", has age {age};"#);
            let pipeline = typeql::parse_query(&query_str).unwrap().into_structure().into_pipeline();
            let (returned_tx, result) = execute_write_query_in_write(
                tx,
                QueryOptions::default_grpc(),
                pipeline,
                query_str,
                ExecutionInterrupt::new_uninterruptible(),
            );
            result.unwrap();
            tx = returned_tx;
        }
        tx.commit().1.unwrap();
        offset += n;
    }
}

// --- Write transaction helpers ---

fn execute_insert_batch(
    database: &Arc<Database<WALClient>>,
    batch_id: usize,
    ops_per_tx: usize,
    timings: &PhaseTimings,
) {
    let t0 = Instant::now();
    let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let t1 = Instant::now();

    for i in 0..ops_per_tx {
        let age: u32 = rand::random();
        let name_id = batch_id * ops_per_tx + i;
        let query_str = format!(r#"insert $p isa person, has name "person_{name_id}", has age {age};"#);
        let pipeline = typeql::parse_query(&query_str).unwrap().into_structure().into_pipeline();
        let (returned_tx, result) = execute_write_query_in_write(
            tx,
            QueryOptions::default_grpc(),
            pipeline,
            query_str,
            ExecutionInterrupt::new_uninterruptible(),
        );
        result.unwrap();
        tx = returned_tx;
    }
    let t2 = Instant::now();

    tx.commit().1.unwrap();
    let t3 = Instant::now();

    timings.record(
        (t1 - t0).as_nanos() as u64,
        (t2 - t1).as_nanos() as u64,
        (t3 - t2).as_nanos() as u64,
    );
}

fn execute_update_batch(
    database: &Arc<Database<WALClient>>,
    batch_id: usize,
    ops_per_tx: usize,
    seed_count: usize,
    timings: &PhaseTimings,
) {
    let t0 = Instant::now();
    let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let t1 = Instant::now();

    for i in 0..ops_per_tx {
        let person_id = (batch_id * ops_per_tx + i) % seed_count;
        let score: f64 = rand::random::<u32>() as f64 / 100.0;
        let query_str = format!(
            r#"match $p isa person, has name "person_{person_id}"; insert $p has score {score};"#
        );
        let pipeline = typeql::parse_query(&query_str).unwrap().into_structure().into_pipeline();
        let (returned_tx, result) = execute_write_query_in_write(
            tx,
            QueryOptions::default_grpc(),
            pipeline,
            query_str,
            ExecutionInterrupt::new_uninterruptible(),
        );
        result.unwrap();
        tx = returned_tx;
    }
    let t2 = Instant::now();

    tx.commit().1.unwrap();
    let t3 = Instant::now();

    timings.record(
        (t1 - t0).as_nanos() as u64,
        (t2 - t1).as_nanos() as u64,
        (t3 - t2).as_nanos() as u64,
    );
}

fn execute_relation_batch(
    database: &Arc<Database<WALClient>>,
    batch_id: usize,
    ops_per_tx: usize,
    seed_count: usize,
    timings: &PhaseTimings,
) {
    let t0 = Instant::now();
    let mut tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let t1 = Instant::now();

    for i in 0..ops_per_tx {
        let idx = batch_id * ops_per_tx + i;
        let a_id = idx % seed_count;
        let b_id = (idx + 1) % seed_count;
        let query_str = format!(
            r#"match $a isa person, has name "person_{a_id}"; $b isa person, has name "person_{b_id}"; insert (friend: $a, friend: $b) isa friendship;"#
        );
        let pipeline = typeql::parse_query(&query_str).unwrap().into_structure().into_pipeline();
        let (returned_tx, result) = execute_write_query_in_write(
            tx,
            QueryOptions::default_grpc(),
            pipeline,
            query_str,
            ExecutionInterrupt::new_uninterruptible(),
        );
        result.unwrap();
        tx = returned_tx;
    }
    let t2 = Instant::now();

    tx.commit().1.unwrap();
    let t3 = Instant::now();

    timings.record(
        (t1 - t0).as_nanos() as u64,
        (t2 - t1).as_nanos() as u64,
        (t3 - t2).as_nanos() as u64,
    );
}

// --- Read transaction helper ---

fn execute_read_query(database: &Arc<Database<WALClient>>, query_str: &str) {
    let tx = TransactionRead::open(database.clone(), TransactionOptions::default()).unwrap();
    let TransactionRead { snapshot, query_manager, type_manager, thing_manager, function_manager, .. } = &tx;
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = query_manager
        .prepare_read_pipeline(snapshot.clone(), type_manager, thing_manager.clone(), function_manager, &query, query_str)
        .unwrap();
    let (rows, _context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let _batch = rows.collect_owned().unwrap();
}

// --- Reporting helpers ---

fn print_header(name: &str, batch_size: usize) {
    eprintln!();
    eprintln!("=== {name} | batch={batch_size} ===");
}

fn print_result(num_threads: usize, elapsed: std::time::Duration, total_ops: usize, timings: &PhaseTimings) {
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    let (open_us, exec_us, commit_us, count) = timings.summary();
    let total_us = open_us + exec_us + commit_us;
    if total_us > 0.0 {
        eprintln!(
            "Threads: {:>3} | Time: {:>8.1}ms | Ops/s: {:>10.0} | \
             avg tx: open {:>8.0}us ({:>4.1}%) exec {:>8.0}us ({:>4.1}%) commit {:>8.0}us ({:>4.1}%) [{} txns]",
            num_threads,
            elapsed.as_secs_f64() * 1000.0,
            ops_per_sec,
            open_us,
            open_us / total_us * 100.0,
            exec_us,
            exec_us / total_us * 100.0,
            commit_us,
            commit_us / total_us * 100.0,
            count,
        );
    } else {
        eprintln!(
            "Threads: {:>3} | Time: {:>8.1}ms | Ops/s: {:>10.0}",
            num_threads,
            elapsed.as_secs_f64() * 1000.0,
            ops_per_sec,
        );
    }
}

fn print_mixed_result(
    num_threads: usize,
    write_threads: usize,
    read_threads: usize,
    elapsed: std::time::Duration,
    write_ops: usize,
    read_ops: usize,
    timings: &PhaseTimings,
) {
    let w_ops_s = write_ops as f64 / elapsed.as_secs_f64();
    let r_ops_s = read_ops as f64 / elapsed.as_secs_f64();
    let (open_us, exec_us, commit_us, count) = timings.summary();
    let total_us = open_us + exec_us + commit_us;
    if total_us > 0.0 {
        eprintln!(
            "Threads: {:>3} ({:>2}W+{:>2}R) | Time: {:>8.1}ms | W-ops/s: {:>10.0} | R-ops/s: {:>10.0} | \
             avg w-tx: open {:>8.0}us exec {:>8.0}us commit {:>8.0}us [{} w-txns]",
            num_threads,
            write_threads,
            read_threads,
            elapsed.as_secs_f64() * 1000.0,
            w_ops_s,
            r_ops_s,
            open_us,
            exec_us,
            commit_us,
            count,
        );
    } else {
        eprintln!(
            "Threads: {:>3} ({:>2}W+{:>2}R) | Time: {:>8.1}ms | W-ops/s: {:>10.0} | R-ops/s: {:>10.0}",
            num_threads,
            write_threads,
            read_threads,
            elapsed.as_secs_f64() * 1000.0,
            w_ops_s,
            r_ops_s,
        );
    }
}

// --- Concurrent runner ---

fn run_write_threads<F>(
    database: &Arc<Database<WALClient>>,
    num_threads: usize,
    ops_per_tx: usize,
    total_ops: usize,
    timings: &Arc<PhaseTimings>,
    thread_fn: F,
) -> std::time::Duration
where
    F: Fn(&Arc<Database<WALClient>>, usize, usize, &PhaseTimings) + Send + Sync + 'static,
{
    let total_transactions = total_ops / ops_per_tx;
    let transactions_per_thread = total_transactions / num_threads;
    let thread_fn = Arc::new(thread_fn);

    let start_signal = Arc::new(RwLock::new(()));
    let write_guard = start_signal.write().unwrap();

    let join_handles: Vec<JoinHandle<()>> = (0..num_threads)
        .map(|thread_id| {
            let db = database.clone();
            let signal = start_signal.clone();
            let timings = timings.clone();
            let thread_fn = thread_fn.clone();
            thread::spawn(move || {
                drop(signal.read().unwrap());
                for batch in 0..transactions_per_thread {
                    let batch_id = thread_id * transactions_per_thread + batch;
                    thread_fn(&db, batch_id, ops_per_tx, &timings);
                }
            })
        })
        .collect();

    let start = Instant::now();
    drop(write_guard);

    for handle in join_handles {
        handle.join().unwrap();
    }

    start.elapsed()
}

// --- W1: Pure Insert ---

fn run_pure_insert_benchmark(thread_counts: &[usize], batch_size: usize) {
    print_header("PureInsert", batch_size);
    for &num_threads in thread_counts {
        if batch_size == 1 && num_threads > 16 {
            continue;
        }
        let (_tmp_dir, database) = create_database(SCHEMA);
        let timings = Arc::new(PhaseTimings::new());
        let total_transactions = TOTAL_OPS / batch_size;
        let transactions_per_thread = total_transactions / num_threads;
        let actual_ops = num_threads * transactions_per_thread * batch_size;

        let elapsed = run_write_threads(&database, num_threads, batch_size, TOTAL_OPS, &timings, |db, batch_id, ops, t| {
            execute_insert_batch(db, batch_id, ops, t);
        });

        print_result(num_threads, elapsed, actual_ops, &timings);
    }
}

// --- W2: Pure Update (match-insert generating Puts) ---

const UPDATE_SEED_COUNT: usize = 10_000;

fn run_pure_update_benchmark(thread_counts: &[usize], batch_size: usize) {
    print_header("PureUpdate", batch_size);
    for &num_threads in thread_counts {
        if batch_size == 1 && num_threads > 16 {
            continue;
        }
        let (_tmp_dir, database) = create_database(SCHEMA);
        seed_persons(&database, UPDATE_SEED_COUNT);
        let timings = Arc::new(PhaseTimings::new());
        let total_transactions = TOTAL_OPS / batch_size;
        let transactions_per_thread = total_transactions / num_threads;
        let actual_ops = num_threads * transactions_per_thread * batch_size;

        let seed_count = UPDATE_SEED_COUNT;
        let elapsed =
            run_write_threads(&database, num_threads, batch_size, TOTAL_OPS, &timings, move |db, batch_id, ops, t| {
                execute_update_batch(db, batch_id, ops, seed_count, t);
            });

        print_result(num_threads, elapsed, actual_ops, &timings);
    }
}

// --- W3: Insert Relations ---

const RELATION_SEED_COUNT: usize = 1_000;

fn run_insert_relation_benchmark(thread_counts: &[usize], batch_size: usize) {
    print_header("InsertRelation", batch_size);
    for &num_threads in thread_counts {
        if batch_size == 1 && num_threads > 16 {
            continue;
        }
        let (_tmp_dir, database) = create_database(SCHEMA);
        seed_persons(&database, RELATION_SEED_COUNT);
        let timings = Arc::new(PhaseTimings::new());
        let total_transactions = TOTAL_OPS / batch_size;
        let transactions_per_thread = total_transactions / num_threads;
        let actual_ops = num_threads * transactions_per_thread * batch_size;

        let seed_count = RELATION_SEED_COUNT;
        let elapsed =
            run_write_threads(&database, num_threads, batch_size, TOTAL_OPS, &timings, move |db, batch_id, ops, t| {
                execute_relation_batch(db, batch_id, ops, seed_count, t);
            });

        print_result(num_threads, elapsed, actual_ops, &timings);
    }
}

// --- W4/W5: Mixed read/write ---

const MIXED_SEED_COUNT: usize = 1_000;

fn run_mixed_benchmark(thread_counts: &[usize], batch_size: usize, write_ratio: f64) {
    let pct = (write_ratio * 100.0) as usize;
    let name = format!("Mixed{pct}Write");
    print_header(&name, batch_size);

    for &num_threads in thread_counts {
        let write_threads = std::cmp::max(1, (num_threads as f64 * write_ratio).round() as usize);
        let read_threads = num_threads - write_threads;
        if read_threads == 0 {
            continue;
        }

        let (_tmp_dir, database) = create_database(SCHEMA);
        seed_persons(&database, MIXED_SEED_COUNT);

        let write_timings = Arc::new(PhaseTimings::new());
        let write_ops_total = Arc::new(AtomicU64::new(0));
        let read_ops_total = Arc::new(AtomicU64::new(0));

        let ops_per_write_tx = batch_size;
        let total_write_txns = TOTAL_OPS / ops_per_write_tx;
        let txns_per_write_thread = total_write_txns / write_threads;

        let running = Arc::new(AtomicBool::new(true));

        let start_signal = Arc::new(RwLock::new(()));
        let write_guard = start_signal.write().unwrap();

        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        // Spawn write threads
        for thread_id in 0..write_threads {
            let db = database.clone();
            let signal = start_signal.clone();
            let timings = write_timings.clone();
            let ops_counter = write_ops_total.clone();
            let seed_count = MIXED_SEED_COUNT;
            handles.push(thread::spawn(move || {
                drop(signal.read().unwrap());
                for batch in 0..txns_per_write_thread {
                    let batch_id = thread_id * txns_per_write_thread + batch;
                    execute_relation_batch(&db, batch_id, ops_per_write_tx, seed_count, &timings);
                    ops_counter.fetch_add(ops_per_write_tx as u64, Ordering::Relaxed);
                }
            }));
        }

        // Spawn read threads
        for _thread_id in 0..read_threads {
            let db = database.clone();
            let signal = start_signal.clone();
            let running = running.clone();
            let ops_counter = read_ops_total.clone();
            handles.push(thread::spawn(move || {
                drop(signal.read().unwrap());
                let mut count: u64 = 0;
                while running.load(Ordering::Relaxed) {
                    let age_threshold = (count % 100) as u32;
                    let query_str = format!(
                        r#"match $p isa person, has age > {age_threshold}, has name $n; limit 10;"#
                    );
                    execute_read_query(&db, &query_str);
                    count += 1;
                }
                ops_counter.fetch_add(count, Ordering::Relaxed);
            }));
        }

        let start = Instant::now();
        drop(write_guard);

        // Wait for write threads, then signal readers to stop
        for handle in handles.drain(..write_threads) {
            handle.join().unwrap();
        }
        running.store(false, Ordering::Relaxed);

        for handle in handles {
            handle.join().unwrap();
        }

        let elapsed = start.elapsed();
        let w_ops = write_ops_total.load(Ordering::Relaxed) as usize;
        let r_ops = read_ops_total.load(Ordering::Relaxed) as usize;

        print_mixed_result(num_threads, write_threads, read_threads, elapsed, w_ops, r_ops, &write_timings);
    }
}

// --- W6: Pure Read ---

const READ_SEED_COUNT: usize = 10_000;

fn run_pure_read_benchmark(thread_counts: &[usize]) {
    print_header("PureRead", 1);

    for &num_threads in thread_counts {
        let (_tmp_dir, database) = create_database(SCHEMA);
        seed_persons(&database, READ_SEED_COUNT);

        let ops_per_thread = READ_OPS / num_threads;
        let actual_ops = ops_per_thread * num_threads;

        let start_signal = Arc::new(RwLock::new(()));
        let write_guard = start_signal.write().unwrap();

        let handles: Vec<JoinHandle<()>> = (0..num_threads)
            .map(|thread_id| {
                let db = database.clone();
                let signal = start_signal.clone();
                thread::spawn(move || {
                    drop(signal.read().unwrap());
                    for i in 0..ops_per_thread {
                        let age_threshold = ((thread_id * ops_per_thread + i) % 100) as u32;
                        let query_str = format!(
                            r#"match $p isa person, has age > {age_threshold}, has name $n; limit 10;"#
                        );
                        execute_read_query(&db, &query_str);
                    }
                })
            })
            .collect();

        let start = Instant::now();
        drop(write_guard);

        for handle in handles {
            handle.join().unwrap();
        }

        let elapsed = start.elapsed();

        eprintln!(
            "Threads: {:>3} | Time: {:>8.1}ms | Reads/s: {:>10.0}",
            num_threads,
            elapsed.as_secs_f64() * 1000.0,
            actual_ops as f64 / elapsed.as_secs_f64(),
        );
    }
}

// --- Main ---

fn main() {
    let write_thread_counts = [1, 2, 4, 8, 16, 32];
    let read_thread_counts = [1, 2, 4, 8, 16, 32, 64];

    eprintln!("Concurrent Write Scalability Benchmark Suite");
    eprintln!("=============================================");
    eprintln!("Total ops per write workload: {TOTAL_OPS}");
    eprintln!("Total ops for pure read:      {READ_OPS}");
    eprintln!();

    // W1: Pure Insert
    for &batch_size in &[1000, 100, 1] {
        run_pure_insert_benchmark(&write_thread_counts, batch_size);
    }

    // W2: Pure Update (match-insert generating Puts)
    for &batch_size in &[1000, 100, 1] {
        run_pure_update_benchmark(&write_thread_counts, batch_size);
    }

    // W3: Insert Relations
    for &batch_size in &[1000, 100, 1] {
        run_insert_relation_benchmark(&write_thread_counts, batch_size);
    }

    // W4: Mixed 50/50
    for &batch_size in &[1000, 100] {
        run_mixed_benchmark(&write_thread_counts, batch_size, 0.5);
    }

    // W5: Mixed 20/80
    for &batch_size in &[1000, 100] {
        run_mixed_benchmark(&write_thread_counts, batch_size, 0.2);
    }

    // W6: Pure Read
    run_pure_read_benchmark(&read_thread_counts);
}
