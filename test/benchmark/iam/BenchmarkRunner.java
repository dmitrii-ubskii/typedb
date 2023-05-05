/*
 * Copyright (C) 2022 Vaticle
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package com.vaticle.typedb.core.reasoner.benchmark.iam;

import com.vaticle.typedb.core.TypeDB;
import com.vaticle.typedb.core.common.parameters.Arguments;
import com.vaticle.typedb.core.common.parameters.Options;
import com.vaticle.typedb.core.common.perfcounter.PerfCounters;
import com.vaticle.typedb.core.database.CoreDatabaseManager;
import com.vaticle.typedb.core.database.CoreTransaction;
import com.vaticle.typedb.core.migrator.data.DataImporter;
import com.vaticle.typedb.core.server.Version;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.vaticle.typedb.core.common.collection.Bytes.MB;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static org.junit.Assert.fail;

public class BenchmarkRunner {
    
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(BenchmarkRunner.class);
    private static final String RESOURCE_DIRECTORY = "test/benchmark/iam/resources/";
    private static CoreDatabaseManager databaseMgr;
    private final String database;

    private static final boolean PRINT_RESULTS = true;
    private final CSVBuilder csvBuilder;

    BenchmarkRunner(String database) {
        this.database = database;
        this.csvBuilder = PRINT_RESULTS ? new CSVBuilder() : null;
    }

    void setUp() throws IOException {
        Path dataDir = Paths.get(System.getProperty("user.dir")).resolve("iam-benchmark-conjunctions");
        if (Files.exists(dataDir)) {
            Files.walk(dataDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
        Files.createDirectory(dataDir);

        databaseMgr = CoreDatabaseManager.open(new Options.Database().dataDir(dataDir).storageDataCacheSize(MB).storageIndexCacheSize(MB));
        databaseMgr.create(database);
    }

    void tearDown() {
        databaseMgr.close();
        if (this.csvBuilder != null) System.out.println(csvBuilder.build());
    }

    private TypeDB.Session schemaSession() {
        return databaseMgr.session(database, Arguments.Session.Type.SCHEMA);
    }

    private TypeDB.Session dataSession() {
        return databaseMgr.session(database, Arguments.Session.Type.DATA);
    }

    void loadSchema(String... filenames) {
        try (TypeDB.Session session = schemaSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.WRITE)) {
                iterate(filenames).forEachRemaining(filename -> {
                    try {
                        TypeQLDefine defineQuery = TypeQL.parseQuery(Files.readString(Paths.get(RESOURCE_DIRECTORY + filename))).asDefine();
                        tx.query().define(defineQuery);
                    } catch (IOException e) {
                        fail("IOException when loading schema: " + e.getMessage());
                    }
                });
                tx.commit();
            }
        }
    }

    void loadData(String filename) {
        new DataImporter(databaseMgr, database, Paths.get(RESOURCE_DIRECTORY + filename), Version.VERSION).run();
    }

    void runBenchmark(Benchmark benchmark) {
        for (int i = 0; i < benchmark.nRuns; i++) {
            BenchmarkRunner.BenchmarkRun run = runMatchQuery(benchmark.query);
            benchmark.addRun(run);
            LOG.info("Completed run in {} ms. answersDiff: {}", run.timeTaken.toMillis(), run.answerCount - benchmark.expectedAnswers);
            LOG.info("perf_counters:\n{}", PerfCounters.prettyPrint(run.reasonerPerfCounters));
        }
        if (csvBuilder != null) csvBuilder.append(benchmark);
    }

    private BenchmarkRun runMatchQuery(String query) {
        BenchmarkRun run;
        try (TypeDB.Session session = dataSession()) {
            try (TypeDB.Transaction tx = session.transaction(Arguments.Transaction.Type.READ, new Options.Transaction().infer(true))) {
                Instant start = Instant.now();
                long nAnswers = tx.query().match(TypeQL.parseQuery(query).asMatch()).count();
                Duration timeTaken = Duration.between(start, Instant.now());
                run = new BenchmarkRun(nAnswers, timeTaken, ((CoreTransaction) tx).reasoner().controllerRegistry().perfCounters().snapshotUnsynchronised());
            }
        }
        return run;
    }

    public void reset() {

    }

    public static class BenchmarkRun {
        final long answerCount;
        final Duration timeTaken;
        final Map<String, Long> reasonerPerfCounters;

        public BenchmarkRun(long answerCount, Duration timeTaken, Map<String, Long> reasonerPerfCounters) {
            this.answerCount = answerCount;
            this.timeTaken = timeTaken;
            this.reasonerPerfCounters = reasonerPerfCounters;
        }

        public List<String> toCSV(Benchmark benchmark, List<String> perfCounterKeys) {
            List<String> entries = new ArrayList<>();
            entries.add(benchmark.name);
            entries.add(Long.toString(benchmark.expectedAnswers));
            entries.add(Long.toString(answerCount));
            entries.add(Long.toString(timeTaken.toMillis()));
            perfCounterKeys.forEach(key -> entries.add(Long.toString(reasonerPerfCounters.get(key))));
            return entries;
        }

        @Override
        public String toString() {
            return "Benchmark run:\n" +
                    "\tTimeTaken :\t" + timeTaken.toMillis() + " ms\n" +
                    "\tAnswers   :\t" + answerCount + "\n" +
                    PerfCounters.prettyPrint(reasonerPerfCounters);
        }
    }

    static class CSVBuilder {

        private final StringBuilder sb;
        private final ArrayList<String> perfCounterKeys;

        CSVBuilder() {
            sb = new StringBuilder();
            List<String> fields = new ArrayList<>();
            Arrays.stream(new String[]{
                    "name", "expectedAnswers", "actualAnswers", "total_time_ms",
            }).forEach(fields::add);
            perfCounterKeys = new ArrayList<>(Benchmark.PERF_KEYS.snapshotUnsynchronised().keySet());
            fields.addAll(perfCounterKeys);
            appendLine(fields);
        }

        public void append(Benchmark benchmark) {
            benchmark.runs.forEach(run -> appendLine(run.toCSV(benchmark, perfCounterKeys)));
        }

        private void appendLine(List<String> entries) {
            entries.forEach(entry -> sb.append(entry).append(","));
            sb.append("\n");
        }

        public String build() {
            return sb.toString();
        }
    }
}
