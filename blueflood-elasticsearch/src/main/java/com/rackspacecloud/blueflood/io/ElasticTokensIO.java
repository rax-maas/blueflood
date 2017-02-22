package com.rackspacecloud.blueflood.io;


import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ElasticClientManager;
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import com.rackspacecloud.blueflood.service.RemoteElasticSearchServer;
import com.rackspacecloud.blueflood.types.Token;
import com.rackspacecloud.blueflood.utils.Metrics;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link TokenDiscoveryIO} implementation using elastic search
 */
public class ElasticTokensIO implements TokenDiscoveryIO {

    public static final String ES_DOCUMENT_TYPE = "tokens";

    protected final Histogram batchHistogram = Metrics.histogram(getClass(), "Batch Sizes");
    protected final Timer writeTimer = Metrics.timer(getClass(), "Write Duration");

    public static String ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE);
    public static String ELASTICSEARCH_TOKEN_INDEX_NAME_READ = Configuration.getInstance().getStringProperty(ElasticIOConfig.ELASTICSEARCH_TOKEN_INDEX_NAME_READ);

    private Client client;

    public ElasticTokensIO() {
        this(RemoteElasticSearchServer.getInstance());
    }

    public ElasticTokensIO(Client client) {
        this.client = client;
    }

    public ElasticTokensIO(ElasticClientManager manager) {
        this(manager.getClient());
    }

    @Override
    public void insertDiscovery(Token token) throws Exception {
        List<Token> batch = new ArrayList<>();
        batch.add(token);
        insertDiscovery(batch);
    }

    @Override
    public void insertDiscovery(List<Token> tokens) throws Exception {
        batchHistogram.update(tokens.size());
        if (tokens.size() == 0) {
            return;
        }

        Timer.Context ctx = writeTimer.time();
        try {

            BulkRequestBuilder bulk = client.prepareBulk();

            for (Token token : tokens) {
                bulk.add(createTokenSingleRequest(token));
            }

            bulk.execute().actionGet();

        } finally {
            ctx.stop();
        }
    }

    @Override
    public List<MetricName> getMetricNames(String tenant, String query) throws Exception {
        return null;
    }

    IndexRequestBuilder createTokenSingleRequest(Token token) throws IOException {

        if (StringUtils.isEmpty(token.getToken())) {
            throw new IllegalArgumentException("trying to insert token discovery without a token");
        }

        return client.prepareIndex(ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE, ES_DOCUMENT_TYPE)
                .setId(token.getDocumentId())
                .setSource(createSourceContent(token))
                .setCreate(true)
                .setRouting(token.getLocator().getTenantId());
    }

    private static XContentBuilder createSourceContent(Token token) throws IOException {
        XContentBuilder json;

        json = XContentFactory.jsonBuilder().startObject()
                              .field(ESFieldLabel.token.toString(), token.getToken())
                              .field(ESFieldLabel.parent.toString(), token.getParent())
                              .field(ESFieldLabel.isLeaf.toString(), token.isLeaf())
                              .field(ESFieldLabel.tenantId.toString(), token.getLocator().getTenantId())
                              .endObject();

        return json;
    }

}
