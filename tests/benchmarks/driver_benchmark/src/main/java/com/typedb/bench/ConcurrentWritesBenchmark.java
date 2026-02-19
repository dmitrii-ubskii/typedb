/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.typedb.bench;

import com.typedb.driver.TypeDB;
import com.typedb.driver.api.Credentials;
import com.typedb.driver.api.Driver;
import com.typedb.driver.api.DriverOptions;
import com.typedb.driver.api.Transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class ConcurrentWritesBenchmark {

    private static final String SERVER_ADDRESS = "127.0.0.1:1729";
    private static final String DB_NAME = "bench-concurrent-writes";

    private static final String SCHEMA = "define\n" +
            "    attribute name value string;\n" +
            "    attribute age value integer;\n" +
            "    entity person owns name, owns age;\n";

    static class PhaseTimings {
        final AtomicLong openNanos = new AtomicLong();
        final AtomicLong executeNanos = new AtomicLong();
        final AtomicLong commitNanos = new AtomicLong();
        final AtomicLong txCount = new AtomicLong();

        void record(long openNs, long executeNs, long commitNs) {
            openNanos.addAndGet(openNs);
            executeNanos.addAndGet(executeNs);
            commitNanos.addAndGet(commitNs);
            txCount.incrementAndGet();
        }
    }

    private static Driver createDriver() {
        return TypeDB.driver(
                SERVER_ADDRESS,
                new Credentials("admin", "password"),
                new DriverOptions(false, null)
        );
    }

    private static void setupDatabase(Driver driver) {
        if (driver.databases().contains(DB_NAME)) {
            driver.databases().get(DB_NAME).delete();
        }
        driver.databases().create(DB_NAME);

        try (Transaction tx = driver.transaction(DB_NAME, Transaction.Type.SCHEMA)) {
            tx.query(SCHEMA).resolve();
            tx.commit();
        }
    }

    private static void executeBatch(Driver driver, int batchId,
                                      int insertsPerTx, PhaseTimings timings) {
        long t0 = System.nanoTime();
        Transaction tx = driver.transaction(DB_NAME, Transaction.Type.WRITE);
        long t1 = System.nanoTime();

        try {
            for (int i = 0; i < insertsPerTx; i++) {
                int age = ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE);
                int nameId = batchId * insertsPerTx + i;
                String query = "insert $p isa person, has name \"person_" + nameId +
                        "\", has age " + age + ";";
                tx.query(query);
            }
            long t2 = System.nanoTime();

            tx.commit();
            long t3 = System.nanoTime();

            timings.record(t1 - t0, t2 - t1, t3 - t2);
        } catch (Exception e) {
            try { tx.close(); } catch (Exception ignored) {}
            throw new RuntimeException("Batch " + batchId + " failed", e);
        }
    }

    private static void runBenchmark(int numThreads, int insertsPerTx, int totalInserts) {
        int totalTransactions = totalInserts / insertsPerTx;
        int transactionsPerThread = totalTransactions / numThreads;

        try (Driver driver = createDriver()) {
            setupDatabase(driver);

            PhaseTimings timings = new PhaseTimings();
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(numThreads);

            List<Thread> threads = new ArrayList<>();
            for (int t = 0; t < numThreads; t++) {
                final int threadId = t;
                Thread thread = new Thread(() -> {
                    try {
                        startLatch.await();
                        for (int batch = 0; batch < transactionsPerThread; batch++) {
                            int batchId = threadId * transactionsPerThread + batch;
                            executeBatch(driver, batchId, insertsPerTx, timings);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        doneLatch.countDown();
                    }
                });
                thread.start();
                threads.add(thread);
            }

            long start = System.nanoTime();
            startLatch.countDown();

            doneLatch.await();
            long elapsed = System.nanoTime() - start;

            int actualInserts = numThreads * transactionsPerThread * insertsPerTx;
            int actualTxns = numThreads * transactionsPerThread;
            double elapsedMs = elapsed / 1_000_000.0;
            double insertsPerSec = actualInserts / (elapsed / 1_000_000_000.0);
            double txnsPerSec = actualTxns / (elapsed / 1_000_000_000.0);

            long count = timings.txCount.get();
            double openUs = count > 0 ? timings.openNanos.get() / (double) count / 1000.0 : 0;
            double execUs = count > 0 ? timings.executeNanos.get() / (double) count / 1000.0 : 0;
            double commitUs = count > 0 ? timings.commitNanos.get() / (double) count / 1000.0 : 0;
            double totalUs = openUs + execUs + commitUs;

            System.err.printf(
                    "Threads: %3d | Time: %8.1fms | Inserts/s: %10.0f | Txns/s: %8.0f | " +
                    "avg tx: open %8.0fus (%4.1f%%) exec %8.0fus (%4.1f%%) commit %8.0fus (%4.1f%%) [%d txns]%n",
                    numThreads, elapsedMs, insertsPerSec, txnsPerSec,
                    openUs, openUs / totalUs * 100,
                    execUs, execUs / totalUs * 100,
                    commitUs, commitUs / totalUs * 100,
                    count
            );

            for (Thread thread : threads) {
                thread.join();
            }

            // Cleanup for next run
            if (driver.databases().contains(DB_NAME)) {
                driver.databases().get(DB_NAME).delete();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        int[] threadCounts1 = {1, 2, 4, 8, 16, 32};
        int[] threadCounts100 = {1, 2, 4, 8, 16, 32, 64, 128};

        System.err.println("Concurrent Write Scalability Benchmark (batch=1)");
        System.err.println("=================================================");
        System.err.printf("Inserts per transaction: %d%n", 1);
        System.err.printf("Total inserts per run:   %d%n", 10_000);
        System.err.println();

        for (int numThreads : threadCounts1) {
            runBenchmark(numThreads, 1, 10_000);
        }

        System.err.println();
        System.err.println("Concurrent Write Scalability Benchmark (batch=100)");
        System.err.println("==================================================");
        System.err.printf("Inserts per transaction: %d%n", 100);
        System.err.printf("Total inserts per run:   %d%n", 100_000);
        System.err.println();

        for (int numThreads : threadCounts100) {
            runBenchmark(numThreads, 100, 100_000);
        }
    }
}
