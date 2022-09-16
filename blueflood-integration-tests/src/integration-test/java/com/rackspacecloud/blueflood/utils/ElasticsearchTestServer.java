package com.rackspacecloud.blueflood.utils;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.io.ElasticIO;
import com.rackspacecloud.blueflood.io.ElasticTokensIO;
import com.rackspacecloud.blueflood.io.EventElasticSearchIO;
import com.rackspacecloud.blueflood.service.Configuration;
import com.rackspacecloud.blueflood.service.ElasticIOConfig;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Arrays;

/**
 * Manages embedded Elasticsearch servers for tests. An instance of this class manages its own, separate Elasticsearch.
 * In theory, multiple instances could be active simultaneously, but at the moment, the tlrx implementation always binds
 * to port 9200, and that's where tests know to find it. To run concurrent instances, that would have to be fixed. The
 * server should bind a random, available port, and each test should ask the instance of this class where to find it.
 *
 * Complication on that last: Blueflood's use of static initialization for lots of internals means that dynamic ports
 * don't work. Some test might start a server on a dynamic port, but some other class has already been initialized and
 * read the original configuration value of the port, so it's looking in the wrong place. It's simpler to just be sure
 * to bind the port(s) that the existing tests know to look for.
 */
public class ElasticsearchTestServer {

    /**
     * Supported ways of starting an Elasticsearch instance for testing. This exists because it's in flux. The
     * tlrx library is old but original to Blueflood. We should use it until we can start upgrading Elasticsearch.
     * Testcontainers seems to be the way to go in the future, and we need to actively migrate toward it. It officially
     * supports version 5.4.0 and newer, but it seems to work fine with older versions, too.
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
    /**
     * ONLY IF using TEST_CONTAINERS as the init method, sets the version of Elasticsearch to test with. This must be
     * a valid Elasticsearch Docker image version.
     */
    private static final String testContainersEsVersion = "1.7";

    private ElasticsearchContainer elasticsearchContainer;
    private EsSetup esSetup;
    private static final ElasticsearchTestServer INSTANCE = new ElasticsearchTestServer();

    public static final ElasticsearchTestServer getInstance() {
        return INSTANCE;
    }

    /**
     * Starts an in-memory Elasticsearch that's configured for Blueflood. If such a server is already running, this is a
     * no-op. The configuration mimics that found in init-es.sh as closely as I can figure out how to.
     */
    public void ensureStarted() {
        if (esInitMethod.equals(EsInitMethod.TEST_CONTAINERS)) {
            if (elasticsearchContainer == null || !elasticsearchContainer.isRunning()) {
                startTestContainer();
            }
        } else if (esInitMethod.equals(EsInitMethod.TLRX)) {
            if (esSetup == null) {
                startTlrx();
            }
        } else if (esInitMethod.equals(EsInitMethod.EXTERNAL)) {
            // Do nothing! You have to manage Elasticsearch your own self!
            System.out.println("Using external Elasticsearch");
        } else {
            throw new IllegalStateException("Illegal value set for Elasticsearch init in tests: " + esInitMethod);
        }
    }

    public void startTestContainer() {
        // Starting with "as compatible" seems to generally work. Why/when do we need to switch to "compatible" images?
        DockerImageName myImage = DockerImageName.parse("elasticsearch:" + testContainersEsVersion)
                .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch");
        elasticsearchContainer = new ElasticsearchContainer(myImage);
        elasticsearchContainer.setPortBindings(Arrays.asList("9200:9200", "9300:9300"));
        elasticsearchContainer.start();
        initIt();
    }

    public void startTlrx() {
        esSetup = new EsSetup();
        esSetup.execute(EsSetup.deleteAll()); //Deletes all the index or documents before creating new ones.
        esSetup.execute(EsSetup.createIndex(ElasticIO.ELASTICSEARCH_INDEX_NAME_WRITE)
                .withSettings(EsSetup.fromClassPath("index_settings.json"))
                .withMapping("metrics", EsSetup.fromClassPath("metrics_mapping.json")));
        esSetup.execute(EsSetup.createIndex(ElasticTokensIO.ELASTICSEARCH_TOKEN_INDEX_NAME_WRITE)
                .withMapping("tokens", EsSetup.fromClassPath("tokens_mapping.json")));
        esSetup.execute(EsSetup.createIndex(EventElasticSearchIO.EVENT_INDEX)
                .withMapping("graphite_event", EsSetup.fromClassPath("events_mapping.json")));
        esSetup.execute(EsSetup.createIndex("blueflood_initialized_marker"));
    }

    private void initIt() {
        String initScript;
        if (testContainersEsVersion.startsWith("1.")) {
            initScript = "init-es.sh";
        } else if (testContainersEsVersion.startsWith("6.")) {
            initScript = "init-es-6/init-es.sh";
        } else {
            throw new IllegalStateException("I don't know which ES init script to use for ES version "
                    + testContainersEsVersion);
        }
        try {
            initIt(initScript);
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException("Failed to run Elasticsearch init script", e);
        }
    }

    private void initIt(String whichScript) throws IOException, InterruptedException {
        // Find the init script, which lives over in the elasticsearch module
        String resourceInThisModule = getClass().getResource("/blueflood.properties").getFile();
        String thisModuleResourcesDir = FilenameUtils.getFullPath(resourceInThisModule);
        String esResourcesDir = thisModuleResourcesDir + "../../../blueflood-elasticsearch/src/main/resources/";
        String initScript = FilenameUtils.concat(esResourcesDir, whichScript);
        String command = initScript + " -u localhost:9200";
        System.out.println("Initialize Elasticsearch for tests with '" + command + "'");
        Process process = Runtime.getRuntime().exec(command);
        IOUtils.copy(process.getInputStream(), System.out);
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IllegalStateException("Elasticsearch init script exited with non-zero status");
        }
    }

    /**
     * Stops the in-memory Elasticsearch managed by this object. It's expected, though unproven, that this would release
     * all resources in use by that Elasticsearch.
     */
    public void stop() {
        if (esInitMethod.equals(EsInitMethod.TEST_CONTAINERS)) {
            elasticsearchContainer.stop();
            elasticsearchContainer = null;
        } else if (esInitMethod.equals(EsInitMethod.TLRX)) {
            esSetup.terminate();
            esSetup = null;
        } else if (esInitMethod.equals(EsInitMethod.EXTERNAL)) {
            // Do nothing! You have to manage Elasticsearch your own self!
            System.out.println("Done with external Elasticsearch");
        } else {
            throw new IllegalStateException("Illegal value set for Elasticsearch init in tests: " + esInitMethod);
        }
    }
}
