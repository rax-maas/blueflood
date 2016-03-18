package com.rackspacecloud.blueflood.threading;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class ThreadPoolSizedExecutorServiceAdapterTest {

    ThreadPoolExecutor mockedExecutor;
    ThreadPoolSizedExecutorServiceAdapter adapter;

    @Before
    public void setUp() {
        mockedExecutor = mock(ThreadPoolExecutor.class);
        adapter = new ThreadPoolSizedExecutorServiceAdapter(mockedExecutor);
    }

    @Test(expected = NullPointerException.class)
    public void nullArgumentThrowsException() {
        new ThreadPoolSizedExecutorServiceAdapter(null);
    }

    @Test
    public void getActiveCountMethodGetsProxied() {

        // when
        adapter.getActiveCount();

        // then
        verify(mockedExecutor, times(1)).getActiveCount();
        verifyNoMoreInteractions(mockedExecutor);
    }

    @Test
    public void getPoolSizeMethodGetsProxied() {

        // when
        adapter.getPoolSize();

        // then
        verify(mockedExecutor, times(1)).getPoolSize();
        verifyNoMoreInteractions(mockedExecutor);
    }

    @Test
    public void shutdownMethodGetsProxied() {

        // when
        adapter.shutdown();

        // then
        verify(mockedExecutor, times(1)).shutdown();
        verifyNoMoreInteractions(mockedExecutor);
    }

    @Test
    public void shutdownNowMethodGetsProxied() {

        // when
        adapter.shutdownNow();

        // then
        verify(mockedExecutor, times(1)).shutdownNow();
        verifyNoMoreInteractions(mockedExecutor);
    }

    @Test
    public void isShutdownMethodGetsProxied() {

        // when
        adapter.isShutdown();

        // then
        verify(mockedExecutor, times(1)).isShutdown();
        verifyNoMoreInteractions(mockedExecutor);
    }

    @Test
    public void isTerminatedMethodGetsProxied() {

        // when
        adapter.isTerminated();

        // then
        verify(mockedExecutor, times(1)).isTerminated();
        verifyNoMoreInteractions(mockedExecutor);
    }

    @Test
    public void awaitTerminationMethodGetsProxied() throws InterruptedException {

        // when
        adapter.awaitTermination(1, TimeUnit.MILLISECONDS);

        // then
        verify(mockedExecutor, times(1)).awaitTermination(1, TimeUnit.MILLISECONDS);
        verifyNoMoreInteractions(mockedExecutor);
    }

    @Test
    public void submit1MethodGetsProxied() {

        // given
        Callable<Object> callable = new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                return null;
            }
        };

        // when
        adapter.submit(callable);

        // then
        verify(mockedExecutor, times(1)).submit(callable);
        verifyNoMoreInteractions(mockedExecutor);
    }

    @Test
    public void submit2MethodGetsProxied() {

        // given
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
            }
        };

        // when
        adapter.submit(runnable, "result");

        // then
        verify(mockedExecutor, times(1)).submit(runnable, "result");
        verifyNoMoreInteractions(mockedExecutor);
    }

    @Test
    public void submit3MethodGetsProxied() {

        // given
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
            }
        };

        // when
        adapter.submit(runnable);

        // then
        verify(mockedExecutor, times(1)).submit(runnable);
        verifyNoMoreInteractions(mockedExecutor);
    }

    @Test
    public void invokeAll1MethodGetsProxied() throws InterruptedException {

        // given
        List<Callable<String>> callables = new ArrayList<Callable<String>>();

        // when
        adapter.invokeAll(callables);

        // then
        verify(mockedExecutor, times(1)).invokeAll(callables);
        verifyNoMoreInteractions(mockedExecutor);
    }

    @Test
    public void invokeAll2MethodGetsProxied() throws InterruptedException {

        // given
        List<Callable<String>> callables = new ArrayList<Callable<String>>();

        // when
        adapter.invokeAll(callables, 1, TimeUnit.MILLISECONDS);

        // then
        verify(mockedExecutor, times(1)).invokeAll(callables, 1, TimeUnit.MILLISECONDS);
        verifyNoMoreInteractions(mockedExecutor);
    }

    @Test
    public void invokeAny1MethodGetsProxied() throws ExecutionException, InterruptedException {

        // given
        List<Callable<String>> callables = new ArrayList<Callable<String>>();

        // when
        adapter.invokeAny(callables);

        // then
        verify(mockedExecutor, times(1)).invokeAny(callables);
        verifyNoMoreInteractions(mockedExecutor);
    }

    @Test
    public void invokeAny2MethodGetsProxied() throws InterruptedException, ExecutionException, TimeoutException {

        // given
        List<Callable<String>> callables = new ArrayList<Callable<String>>();

        // when
        adapter.invokeAny(callables, 1, TimeUnit.MILLISECONDS);

        // then
        verify(mockedExecutor, times(1)).invokeAny(callables, 1, TimeUnit.MILLISECONDS);
        verifyNoMoreInteractions(mockedExecutor);
    }

    @Test
    public void executeMethodGetsProxied() {

        // given
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
            }
        };

        // when
        adapter.execute(runnable);

        // then
        verify(mockedExecutor, times(1)).execute(runnable);
        verifyNoMoreInteractions(mockedExecutor);
    }
}
