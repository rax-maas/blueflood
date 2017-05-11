package com.rackspacecloud.blueflood.http;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.rackspacecloud.blueflood.io.*;
import com.rackspacecloud.blueflood.service.CoreConfig;
import com.rackspacecloud.blueflood.utils.ModuleLoader;
import org.apache.http.annotation.NotThreadSafe;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Scanner;
import java.util.concurrent.ExecutionException;

@ThreadLeakScope(ThreadLeakScope.Scope.NONE) //https://github.com/elastic/elasticsearch/issues/8642
@NotThreadSafe
public class HttpESIntegrationBase extends ESIntegTestCase {

    private String convertStreamToString(java.io.InputStream is) {
        Scanner s = new Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }

    private String getDataAsString(String fileName) {
        return convertStreamToString(HttpESIntegrationBase.class
                                             .getClassLoader()
                                             .getResourceAsStream(fileName));
    }

    private String getIndexSettings() {
        return getDataAsString("index_settings.json");
    }

    protected String getMetricsMapping() {
        return getDataAsString("metrics_mapping.json");
    }

    protected String getTokensMapping() {
        return getDataAsString("tokens_mapping.json");
    }

    protected String getEventsMapping() {
        return getDataAsString("events_mapping.json");
    }

    protected void createIndexAndMapping(String indexName, String indexType, String fieldMappings)
            throws ExecutionException, InterruptedException {

        client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.settingsBuilder().loadFromSource(getIndexSettings()))
                .addMapping(indexType, fieldMappings)
                .execute()
                .get();
    }

    protected void addAlias(String indexName, String aliasName) throws ExecutionException, InterruptedException {
        client().admin()
                .indices()
                .prepareAliases()
                .addAlias(indexName, aliasName)
                .execute()
                .get();
    }

    protected void refreshChanges() {
        flushAndRefresh();
    }

    protected Client getClient() {
        return client();
    }

    protected void esSetup() throws ExecutionException, InterruptedException {
        // setup elasticsearch test clusters with blueflood mappings for events
        createIndexAndMapping(EventElasticSearchIO.EVENT_INDEX,
                              "graphite_event",
                              getEventsMapping());

        // setup elasticsearch test clusters with blueflood mappings for metric discovery
        createIndexAndMapping(ElasticIO.ELASTICSEARCH_INDEX_NAME_WRITE,
                              ElasticIO.ES_DOCUMENT_TYPE,
                              getMetricsMapping());

        // setup elasticsearch test clusters with blueflood mappings for metric tokens discovery
        createIndexAndMapping(ElasticTokensIO.ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE,
                              ElasticTokensIO.ES_DOCUMENT_TYPE,
                              getTokensMapping());

        refreshChanges();

        //setting ES client back in ElasticIO and ElasticTokensIO so that ingestion service can interact with ES test cluster.
        ((ElasticIO) ModuleLoader.getInstance(DiscoveryIO.class, CoreConfig.DISCOVERY_MODULES)).setClient(getClient());
        ((ElasticTokensIO) ModuleLoader.getInstance(TokenDiscoveryIO.class, CoreConfig.TOKEN_DISCOVERY_MODULES)).setClient(getClient());
    }

}
