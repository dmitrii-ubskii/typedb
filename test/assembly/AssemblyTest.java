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

package com.vaticle.typedb.core.test.assembly;

import com.vaticle.typedb.console.tool.runner.TypeDBConsoleRunner;
import com.vaticle.typedb.core.tool.runner.TypeDBCoreRunner;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import static com.vaticle.typedb.common.collection.Collections.map;
import static com.vaticle.typedb.common.collection.Collections.pair;

public class AssemblyTest {

    @Test
    public void bootup() throws InterruptedException, TimeoutException, IOException {
        TypeDBCoreRunner server = new TypeDBCoreRunner(map(
            pair("--diagnostics.reporting.errors", "false"),
            pair("--diagnostics.monitoring.enable", "false")
        ));
        try {
            server.start();
        } finally {
            server.stop();
        }
    }

    @Test
    public void console() throws InterruptedException, TimeoutException, IOException {
        TypeDBCoreRunner server = new TypeDBCoreRunner(map(
            pair("--diagnostics.reporting.errors", "false"),
            pair("--diagnostics.monitoring.enable", "false")
        ));
        try {
            server.start();
            TypeDBConsoleRunner console = new TypeDBConsoleRunner();
            int exitCode = console.run(
                    "--core", server.address(),
                    "--script", Paths.get("test", "assembly", "console-script").toAbsolutePath().toString()
            );
            assert exitCode == 0;
        } finally {
            server.stop();
        }
    }
}
