package com.rackspacecloud.blueflood.utils;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.io.ElasticIO;
import com.rackspacecloud.blueflood.io.ElasticTokensIO;
import com.rackspacecloud.blueflood.io.EventElasticSearchIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Manages embedded Elasticsearch servers for tests. An instance of this class manages its own, separate Elasticsearch.
 * In theory, multiple instances could be active simultaneously, but at the moment, the tlrx implementation always binds
 * to port 9200, and that's where tests know to find it. To run concurrent instances, that would have to be fixed. The
 * server should bind a random, available port, and each test should ask the instance of this class where to find it.
 */
public class ElasticsearchTestServer {

    /**
     * Supported ways of starting an Elasticsearch instance for testing. This exists because it's in flux. The
     * tlrx library is old but original to Blueflood. We should use it until we can start upgrading Elasticsearch.
     * Testcontainers seems to be the way to go in the future, but it officially supports Elasticsearch no older than
     * 5.4.0. We may be able to force it to use an older one. For now, we need to get the project running stably and
     * predictably.
     *
     * EXTERNAL means Elasticsearch will be started externally. Using EXTERNAL will turn this class into a no-op. It
     * won't try to start or stop anything, and everything will be left up to you to manage yourself.
     */
    private enum EsInitMethod {
        // Probably the best way to test against Elasticsearch for the foreseeable future. Testcontainers is a library
        // that provides many on-demand services for testing purposes via Docker. The oldest Elasticsearch that it
        // officially supports is 5.4.0. We may be able to use an older image with it, too, but we'll need to verify.
        //
        // Note: This method will definitely not work with the testcontainers 5.4.0 image until ElasticsearchRestHelper
        // is updated to support authentication!
        TEST_CONTAINERS,

        // The old way of testing against Elasticsearch used elsewhere in the project. It seems to start an in-memory
        // instance. This library hasn't been maintained in years, so we need to stop using it for testing new versions
        // of Elasticsearch.
        TLRX,

        // Indicates that you'll start Elasticsearch externally, like with Docker. See the 10-minute guide on the wiki
        // for a quick startup. This test class will do nothing in terms of starting or initializing Elasticsearch when
        // you use this option.
        EXTERNAL
    }

    /**
     * The selected way to start Elasticsearch for this test class. This will change over time as we update
     * Elasticsearch and change to new testing mechanisms. It's useful to declare here so that you can easily test
     * against different variants in a dev environment.
     */
    private static final EsInitMethod esInitMethod = EsInitMethod.TLRX;

    private ElasticsearchContainer elasticsearchContainer;
    private EsSetup esSetup;

    /**
     * Starts an in-memory Elasticsearch that's configured for Blueflood. The configuration mimics that found in
     * init-es.sh as closely as I can figure out how to.
     */
    public void start() {
        if (esInitMethod.equals(EsInitMethod.TEST_CONTAINERS)) {
            startTestContainer();
        } else if (esInitMethod.equals(EsInitMethod.TLRX)) {
            startTlrx();
        } else if (esInitMethod.equals(EsInitMethod.EXTERNAL)) {
            // Do nothing! You have to manage Elasticsearch your own self!
            System.out.println("Using external Elasticsearch");
        } else {
            throw new IllegalStateException("Illegal value set for Elasticsearch init in tests: " + esInitMethod);
        }
    }

    public void startTestContainer() {
        // Try to start an old version. Does this work?
        DockerImageName myImage = DockerImageName.parse("elasticsearch:1.7")
                .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch");
        elasticsearchContainer = new ElasticsearchContainer(myImage);

        // Or, with the officially supported version:
        // elasticsearchContainer = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:5.4.0");

        elasticsearchContainer.start();

        // TODO: Create the indexes and mappings as seen in init-es.sh

        // The container starts on a random, unused port. Configure the rest client to use the correct port.
        Configuration.getInstance().setProperty(
                ElasticIOConfig.ELASTICSEARCH_HOST_FOR_REST_CLIENT, elasticsearchContainer.getHttpHostAddress());
    }

    public void startTlrx() {
        esSetup = new EsSetup();
        esSetup.execute(EsSetup.createIndex(ElasticIO.ELASTICSEARCH_INDEX_NAME_WRITE)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping("metrics", EsSetup.fromClassPath("metrics_mapping.json")));
        esSetup.execute(EsSetup.createIndex(ElasticTokensIO.ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE)
                .withMapping("tokens", EsSetup.fromClassPath("tokens_mapping.json")));
        esSetup.execute(EsSetup.createIndex(EventElasticSearchIO.EVENT_INDEX)
                .withMapping("graphite_event", EsSetup.fromClassPath("events_mapping.json")));
        esSetup.execute(EsSetup.createIndex("blueflood_initialized_marker"));
    }

    /**
     * Stops the in-memory Elasticsearch managed by this object. It's expected, though unproven, that this would release
     * all resources in use by that Elasticsearch.
     */
    public void stop() {
        if (esInitMethod.equals(EsInitMethod.TEST_CONTAINERS)) {
            elasticsearchContainer.stop();
        } else if (esInitMethod.equals(EsInitMethod.TLRX)) {
            esSetup.terminate();
        } else if (esInitMethod.equals(EsInitMethod.EXTERNAL)) {
            // Do nothing! You have to manage Elasticsearch your own self!
            System.out.println("Done with external Elasticsearch");
        } else {
            throw new IllegalStateException("Illegal value set for Elasticsearch init in tests: " + esInitMethod);
        }
    }
}
