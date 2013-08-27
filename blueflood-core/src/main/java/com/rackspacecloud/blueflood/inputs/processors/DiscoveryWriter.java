package com.rackspacecloud.blueflood.inputs.processors;

import com.google.common.util.concurrent.ListenableFuture;
import com.rackspacecloud.blueflood.concurrent.AsyncFunctionWithThreadPool;
import com.rackspacecloud.blueflood.concurrent.NoOpFuture;
import com.rackspacecloud.blueflood.io.ElasticIO;
import com.rackspacecloud.blueflood.types.Locator;
import com.rackspacecloud.blueflood.types.Metric;
import static com.rackspacecloud.blueflood.io.ElasticIO.InsertRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadPoolExecutor;

public class DiscoveryWriter extends AsyncFunctionWithThreadPool<List<List<Metric>>, List<List<Metric>>> {

	public DiscoveryWriter(ThreadPoolExecutor threadPool) {
		super(threadPool);
	}

	@Override
	public ListenableFuture<List<List<Metric>>> apply(List<List<Metric>> input) throws Exception {

		for (final List<Metric> metrics : input) {
			
				getThreadPool().submit(new Callable<Void>() {
					@Override
					public Void call() {
						for (Metric metric : metrics) {
							final Locator locator = metric.getLocator();
							InsertRequest request = new InsertRequest.Builder(locator.getMetricName()).build();
						}
						return null;
					}
				});
		}

		return new NoOpFuture<List<List<Metric>>>(input);
	}
}
