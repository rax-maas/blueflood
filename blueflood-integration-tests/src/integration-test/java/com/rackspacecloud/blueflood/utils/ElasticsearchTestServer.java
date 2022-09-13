package com.rackspacecloud.blueflood.utils;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.IntegrationTestConfig;
import com.rackspacecloud.blueflood.io.ElasticIO;
import com.rackspacecloud.blueflood.io.ElasticTokensIO;
import com.rackspacecloud.blueflood.io.EventElasticSearchIO;
import com.rackspacecloud.blueflood.service.Configuration;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Manages embedded Elasticsearch servers for tests. An instance of this class manages its own, separate Elasticsearch.
 * In theory, multiple instances could be active simultaneously, but at the moment, the tlrx implementation always binds
 * to port 9200, and that's where tests know to find it. To run concurrent instances, that would have to be fixed. The
 * server should bind a random, available port, and each test should ask the instance of this class where to find it.
 *
 * Complication to that last: since blueflood relies heavily on static class initialization, it's not really feasible to
 * use dynamic port binding. By the time a port is chosen, it's very likely that some other class has already read the
 * old configuration value for the port and would then be looking at the wrong place.
 */
public class ElasticsearchTestServer {
    private final String esInitMethod =
            Configuration.getInstance().getStringProperty(IntegrationTestConfig.IT_ELASTICSEARCH_TEST_METHOD);
    private final String testContainersEsVersion =
            Configuration.getInstance().getStringProperty(IntegrationTestConfig.IT_ELASTICSEARCH_CONTAINER_VERSION);
    private static final ElasticsearchTestServer INSTANCE = new ElasticsearchTestServer();

    private ElasticsearchContainer elasticsearchContainer;
    private EsSetup esSetup;

    /**
     * Provides a global singleton so that all tests can share a single Elasticsearch instance instead of each test
     * starting its own. It's not as slow as Cassandra, but the initialization takes some time, so it's best if we only
     * start it once per entire suite of tests.
     */
    public static ElasticsearchTestServer getInstance() {
        return INSTANCE;
    }

    /**
     * Starts an in-memory Elasticsearch that's configured for Blueflood. If this instance has such a server already
     * running, this is a no-op.
     */
    public boolean ensureStarted() {
        if (esInitMethod.equals("TEST_CONTAINERS")) {
            if (elasticsearchContainer == null || !elasticsearchContainer.isRunning()) {
                startTestContainer();
                return true;
            }
        } else if (esInitMethod.equals("TLRX")) {
            if (esSetup == null) {
                startTlrx();
                return true;
            }
        } else if (esInitMethod.equals("EXTERNAL")) {
            // Do nothing! You have to manage Elasticsearch your own self!
            System.out.println("Using external Elasticsearch");
        } else {
            throw new IllegalStateException("Illegal config value for IT_ELASTICSEARCH_TEST_METHOD: " + esInitMethod);
        }
        return false;
    }

    /**
     * Same as {@link #ensureStarted()}, but cleans all indexes before returning. Tests should prefer to use randomized
     * data to avoid conflicts with other tests, but older tests don't necessarily obey this principle.
     */
    public void ensureStartedClean() {
        boolean started = ensureStarted();
        // Only re-init if it wasn't a fresh start.
        if (!started) {
            initIt(true);
        }
    }

    public void startTestContainer() {
        System.out.println("Starting Elasticsearch test server with TestContainers");
        DockerImageName imageName = DockerImageName.parse("elasticsearch:" + testContainersEsVersion)
                .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch");
        elasticsearchContainer = new ElasticsearchContainer(imageName);
        elasticsearchContainer.setPortBindings(Arrays.asList("9200:9200", "9300:9300"));
        elasticsearchContainer.start();
        initIt(false);
    }

    private void initIt(boolean reset) {
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
            initIt(initScript, reset);
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException("Failed to run Elasticsearch init script", e);
        }
    }

    private void initIt(String whichScript, boolean reset) throws IOException, InterruptedException {
        // Find the init script, which lives over in the elasticsearch module
        String resourceInThisModule = getClass().getResource("/blueflood.properties").getFile();
        String thisModuleResourcesDir = FilenameUtils.getFullPath(resourceInThisModule);
        String esResourcesDir = thisModuleResourcesDir + "../../../blueflood-elasticsearch/src/main/resources/";
        String initScript = FilenameUtils.concat(esResourcesDir, whichScript);
        String command = initScript + " -u localhost:9200";
        if (reset) {
            command += " -r";
        }
        System.out.println("Initialize Elasticsearch for tests with " + command);
        Process process = Runtime.getRuntime().exec(command);
        IOUtils.copy(process.getInputStream(), System.out);
        int exit = process.waitFor();
        if (exit != 0) {
            throw new IllegalStateException("Elasticsearch init script exited with non-zero status");
        }
    }

    public void startTlrx() {
        System.out.println("Starting Elasticsearch test server with TLRX");
        esSetup = new EsSetup();
        // It doesn't seem like this line should be necessary, but it seems Elasticsearch fails to start without it.
        // Look forward to when we can migrate to test containers and this just goes away.
        esSetup.execute(EsSetup.deleteAll());
        initIt(false);
    }

    /**
     * Stops the in-memory Elasticsearch managed by this object. This should mostly never be called. Let the same server
     * run for a whole suite of tests. It's faster that way. It's expected, though unproven, that this would release all
     * resources in use by that Elasticsearch.
     */
    public void stop() {
        if (esInitMethod.equals("TEST_CONTAINERS")) {
            elasticsearchContainer.stop();
            elasticsearchContainer = null;
        } else if (esInitMethod.equals("TLRX")) {
            esSetup.terminate();
            esSetup = null;
        } else if (esInitMethod.equals("EXTERNAL")) {
            // Do nothing! You have to manage Elasticsearch your own self!
            System.out.println("Done with external Elasticsearch");
        } else {
            throw new IllegalStateException("Illegal config value for IT_ELASTICSEARCH_TEST_METHOD: " + esInitMethod);
        }
    }
}
