package com.rackspacecloud.blueflood.threading;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * An adapter class so that we can use {@code ThreadPoolExecutor} with
 * {@link SizedExecutorService}, to support dependency injection in the tests.
 *
 * TODO:
 * {@link com.rackspacecloud.blueflood.service.RollupBatchWriter#enqueueRollupForWrite}
 * makes calls to {@link #getActiveCount()} and {@link #getPoolSize()}, which
 * means it must be hard-coded to use the {@link ThreadPoolExecutor} class
 * instead of the {@link ExecutorService} interface. Once that method is fixed
 * and no longer relies on those calls, it can be changed to use just
 * {@link ExecutorService}, and both
 * {@link ThreadPoolSizedExecutorServiceAdapter} and
 * {@link SizedExecutorService} should be removed.
 */
public class ThreadPoolSizedExecutorServiceAdapter implements SizedExecutorService {

    private final ThreadPoolExecutor executor;

    public ThreadPoolSizedExecutorServiceAdapter(ThreadPoolExecutor executor) {
        if (executor == null) throw new NullPointerException();
        this.executor = executor;
    }

    @Override
    public int getActiveCount() {
        return this.executor.getActiveCount();
    }

    @Override
    public int getPoolSize() {
        return this.executor.getPoolSize();
    }

    @Override
    public void shutdown() {
        this.executor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return this.executor.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return this.executor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return this.executor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return this.executor.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return this.executor.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return this.executor.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return this.executor.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return this.executor.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return this.executor.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return this.executor.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return this.executor.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        this.executor.execute(command);
    }
}
