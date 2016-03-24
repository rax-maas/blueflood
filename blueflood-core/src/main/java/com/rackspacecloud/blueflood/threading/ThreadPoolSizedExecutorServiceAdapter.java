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

    /**
     * {@inheritDoc}
     */
    @Override
    public int getActiveCount() {
        return executor.getActiveCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getPoolSize() {
        return executor.getPoolSize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        executor.shutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Runnable> shutdownNow() {
        return executor.shutdownNow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isShutdown() {
        return executor.isShutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTerminated() {
        return executor.isTerminated();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executor.awaitTermination(timeout, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return executor.submit(task);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return executor.submit(task, result);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<?> submit(Runnable task) {
        return executor.submit(task);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return executor.invokeAll(tasks);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return executor.invokeAll(tasks, timeout, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return executor.invokeAny(tasks);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return executor.invokeAny(tasks, timeout, unit);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Runnable command) {
        executor.execute(command);
    }
}
