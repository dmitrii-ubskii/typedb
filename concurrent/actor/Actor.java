/*
 * Copyright (C) 2021 Grakn Labs
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
 */

package grakn.core.concurrent.actor;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

@ThreadSafe
public abstract class Actor<ACTOR extends Actor<ACTOR>> {

    private static final String ERROR_ACTOR_DRIVER_IS_NULL = "driver() must not be null.";
    private final Driver<ACTOR> driver;
    private final String name;

    public static <NEW_ACTOR extends Actor<NEW_ACTOR>> Driver<NEW_ACTOR> driver(
            Function<Driver<NEW_ACTOR>, NEW_ACTOR> actorFn, ActorExecutorService executorService) {
        Driver<NEW_ACTOR> actor = new Driver<>(executorService);
        actor.actor = actorFn.apply(actor);
        return actor;
    }

    protected abstract void exception(Throwable e);

    protected Actor(Driver<ACTOR> driver, String name) {
        this.driver = driver;
        this.name = name;
    }

    protected Driver<ACTOR> driver() {
        assert this.driver != null : ERROR_ACTOR_DRIVER_IS_NULL;
        return this.driver;
    }

    public String name() {
        return name;
    }

    public static class Driver<ACTOR extends Actor<ACTOR>> {

        private static final String ERROR_ACTOR_NOT_SETUP =
                "Attempting to access the Actor, but it is not yet setup. " +
                "Are you trying to send a message to yourself within the constructor?";

        private ACTOR actor;
        private final ActorExecutorService executorService;
        private final ActorExecutor executor;

        private Driver(ActorExecutorService executorService) {
            this.executorService = executorService;
            this.executor = executorService.nextExecutor();
        }

        // TODO: do not use this method - any usages should be removed ASAP
        public ACTOR actor() {
            return actor;
        }

        public String name() {
            return actor.name();
        }

        public void execute(Consumer<ACTOR> job) {
            assert actor != null : ERROR_ACTOR_NOT_SETUP;
            executor.submit(() -> job.accept(actor), actor::exception);
        }

        public CompletableFuture<Void> complete(Consumer<ACTOR> job) {
            return compute(actor -> {
                job.accept(actor);
                return null;
            });
        }

        public <ANSWER> CompletableFuture<ANSWER> compute(Function<ACTOR, ANSWER> job) {
            assert actor != null : ERROR_ACTOR_NOT_SETUP;
            CompletableFuture<ANSWER> future = new CompletableFuture<>();
            executor.submit(
                    () -> future.complete(job.apply(actor)),
                    e -> {
                        actor.exception(e);
                        future.completeExceptionally(e);
                    }
            );
            return future;
        }

    public ActorExecutor.FutureTask schedule(Consumer<ACTOR> job, long scheduleMillis) {
        assert actor != null : ERROR_ACTOR_NOT_SETUP;
        return executor.schedule(() -> job.accept(actor), scheduleMillis, actor::exception);
    }

        public ActorExecutorService executorService() {
            return executorService;
        }

        public ActorExecutor executor() {
            return executor;
        }
    }
}
