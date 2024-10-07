/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package com.vaticle.typedb.core.concurrent.executor;

import com.vaticle.typedb.common.concurrent.NamedThreadFactory;
import com.vaticle.typedb.core.common.exception.TypeDBException;
import com.vaticle.typedb.core.concurrent.actor.ActorExecutorGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_ARGUMENT;
import static com.vaticle.typedb.core.common.exception.ErrorMessage.Internal.ILLEGAL_OPERATION;

public class Executors {

    public static final int SHUTDOWN_TIMEOUT_MS = 1000;

    public static int PARALLELISATION_FACTOR = -1;

    private static final Logger LOG = LoggerFactory.getLogger(Executors.class);
    private static final String TYPEDB_CORE_SERVICE_THREAD_NAME = "typedb-service";
    private static final String TYPEDB_CORE_TRANSACTION_SERVICE_THREAD_NAME = "typedb-transaction-service";
    private static final String TYPEDB_CORE_ASYNC_THREAD_1_NAME = "typedb-async-1";
    private static final String TYPEDB_CORE_ASYNC_THREAD_2_NAME = "typedb-async-2";
    private static final String TYPEDB_CORE_NETWORK_THREAD_NAME = "typedb-network";
    private static final String TYPEDB_CORE_ACTOR_THREAD_NAME = "typedb-actor";
    private static final String TYPEDB_CORE_SERIAL_THREAD_NAME = "typedb-serial";
    private static final String TYPEDB_CORE_SCHEDULED_THREAD_NAME = "typedb-scheduled";
    private static final int TYPEDB_CORE_SCHEDULED_THREAD_SIZE = 1;

    private static Executors singleton = null;

    private final ParallelThreadPoolExecutor serviceExecutorService;
    private final ParallelThreadPoolExecutor transactionExecutorService;
    private final ParallelThreadPoolExecutor asyncExecutorService1;
    private final ParallelThreadPoolExecutor asyncExecutorService2;
    private final ActorExecutorGroup actorExecutorService;
    private final NioEventLoopGroup networkExecutorService;
    private final ScheduledThreadPoolExecutor scheduledThreadPool;
    private final ExecutorService serialService;

    private Executors(int parallelisation) {
        if (parallelisation <= 0) throw TypeDBException.of(ILLEGAL_ARGUMENT);
        serviceExecutorService = new ParallelThreadPoolExecutor(parallelisation, threadFactory(TYPEDB_CORE_SERVICE_THREAD_NAME));
        transactionExecutorService = new ParallelThreadPoolExecutor(parallelisation, threadFactory(TYPEDB_CORE_TRANSACTION_SERVICE_THREAD_NAME));
        asyncExecutorService1 = new ParallelThreadPoolExecutor(parallelisation, threadFactory(TYPEDB_CORE_ASYNC_THREAD_1_NAME));
        asyncExecutorService2 = new ParallelThreadPoolExecutor(parallelisation, threadFactory(TYPEDB_CORE_ASYNC_THREAD_2_NAME));
        actorExecutorService = new ActorExecutorGroup(parallelisation, threadFactory(TYPEDB_CORE_ACTOR_THREAD_NAME));
        networkExecutorService = new NioEventLoopGroup(parallelisation, threadFactory(TYPEDB_CORE_NETWORK_THREAD_NAME));
        scheduledThreadPool = new ScheduledThreadPoolExecutor(TYPEDB_CORE_SCHEDULED_THREAD_SIZE,
                                                              threadFactory(TYPEDB_CORE_SCHEDULED_THREAD_NAME));
        serialService = java.util.concurrent.Executors.newSingleThreadExecutor(threadFactory(TYPEDB_CORE_SERIAL_THREAD_NAME));
        scheduledThreadPool.setRemoveOnCancelPolicy(true);
    }

    public static Executor transactionService() {
        assert isInitialised();
        return singleton.transactionExecutorService;
    }

    private NamedThreadFactory threadFactory(String threadNamePrefix) {
        return NamedThreadFactory.create(threadNamePrefix);
    }

    public static synchronized void initialise(int parallelisationFactor) {
        if (isInitialised()) throw TypeDBException.of(ILLEGAL_OPERATION);
        PARALLELISATION_FACTOR = parallelisationFactor;
        singleton = new Executors(parallelisationFactor);
    }

    public static boolean isInitialised() {
        return singleton != null;
    }

    public static ParallelThreadPoolExecutor service() {
        assert isInitialised();
        return singleton.serviceExecutorService;
    }

    public static ParallelThreadPoolExecutor async1() {
        assert isInitialised();
        return singleton.asyncExecutorService1;
    }

    public static ParallelThreadPoolExecutor async2() {
        assert isInitialised();
        return singleton.asyncExecutorService2;
    }

    public static ActorExecutorGroup actor() {
        assert isInitialised();
        return singleton.actorExecutorService;
    }

    public static NioEventLoopGroup network() {
        assert isInitialised();
        return singleton.networkExecutorService;
    }

    public static ScheduledThreadPoolExecutor scheduled() {
        assert isInitialised();
        return singleton.scheduledThreadPool;
    }

    public static ExecutorService serial() {
        assert isInitialised();
        return singleton.serialService;
    }
}
