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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class LargeDataTest extends ReasonerBenchmarkSuite {

    private static final String database = "iam-benchmark-data";

    public LargeDataTest() {
        super(database);
    }

    @Before
    public void setUp() throws IOException {
        benchmarker.setUp();
        benchmarker.loadSchema("schema_types.tql");
        benchmarker.loadSchema("schema_rules_optimised.tql");
        benchmarker.loadData("data_small.typedb");
    }

    @After
    public void tearDown() {
        benchmarker.tearDown();
    }

    @Test
    public void testHighSelectivity() {
        String query = "match\n" +
                "   $po (action: $a1, action: $a2) isa segregation-policy;\n" +
                "   $ac1 (object: $o, action: $a1) isa access;\n" +
                "   $ac2 (object: $o, action: $a2) isa access;\n" +
                "   $p1 (subject: $s, access: $ac1) isa permission;\n" +
                "   $p2 (subject: $s, access: $ac2) isa permission;\n";
        Benchmark benchmark = new Benchmark("high-selectivity", query, 1);
        runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
    }

    @Test
    public void testCombinatorialResults() {
        String query = "match\n" +
        "   $p1 (subject: $s1, access: $ac1) isa permission;\n" +
        "   $p2 (subject: $s2, access: $ac2) isa permission;\n";
        Benchmark benchmark = new Benchmark("combinatorial-results", query, 1);
        runBenchmark(benchmark);
        benchmark.assertAnswerCountCorrect();
    }

    @Test
    public void testLargeNegations() {
        // TODO
    }
}
