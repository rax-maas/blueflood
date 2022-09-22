package com.rackspacecloud.blueflood.utils;

import com.github.tlrx.elasticsearch.test.EsSetup;
import com.rackspacecloud.blueflood.IntegrationTestConfig;
import com.rackspacecloud.blueflood.service.Configuration;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Manages embedded Elasticsearch servers for tests. An instance of this class manages its own, separate Elasticsearch.
 * The instance is initialized using files from the blueflood-elasticsearch module's src/main/resources. We expect to
 * have a consistent set of files available for each version of Elasticsearch so that we can easily switch versions and
 * locate the correct files for configuring it.
 *
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

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchTestServer.class);

    private final String esInitMethod =
            Configuration.getInstance().getStringProperty(IntegrationTestConfig.IT_ELASTICSEARCH_TEST_METHOD);
    private final String testContainersEsVersion =
            Configuration.getInstance().getStringProperty(IntegrationTestConfig.IT_ELASTICSEARCH_CONTAINER_VERSION);

    private ElasticsearchContainer elasticsearchContainer;
    private EsSetup esSetup;
    /**
     * An http client, handy for talking to Elasticsearch. Keeping one on hand is more efficient than creating one every
     * time you need it.
     */
    private final CloseableHttpClient client = HttpClientBuilder.create().build();
    private static final ElasticsearchTestServer INSTANCE = new ElasticsearchTestServer();

    public static final ElasticsearchTestServer getInstance() {
        return INSTANCE;
    }

    /**
     * Starts an in-memory Elasticsearch that's configured for Blueflood. If such a server is already running, this is a
     * no-op. The configuration mimics that found in init-es.sh as closely as I can figure out how to.
     */
    public void ensureStarted() {
        if (esInitMethod.equals("TEST_CONTAINERS")) {
            if (elasticsearchContainer == null || !elasticsearchContainer.isRunning()) {
                startTestContainer();
            }
        } else if (esInitMethod.equals("TLRX")) {
            if (esSetup == null) {
                startTlrx();
            }
        } else if (esInitMethod.equals("EXTERNAL")) {
            // Do nothing! You have to manage Elasticsearch your own self!
            log.info("Using external Elasticsearch");
        } else {
            throw new IllegalStateException("Illegal value set for Elasticsearch init in tests: " + esInitMethod);
        }
    }

    /**
     * Creates a new index for testing. You shouldn't need to do this unless you specifically want to test something
     * about an index outside the normal Blueflood indexes. The normal indexes are created when the test server starts
     * or is reset.
     *
     * If you do need to create a different index, pass the index name, type name, and name of a mapping file. The
     * mapping file must be one of those found in src/main/resource in blueflood-elasticsearch, or one of the versioned
     * init-es-* subdirectories, as determined by the current value of IT_ELASTICSEARCH_CONTAINER_VERSION.
     *
     * The type name you pass *must* match the type name specified in the mapping file.
     */
    public void createIndex(String name, String type, String mappingFileName) {
        try {
            String indexUri = "http://localhost:9200/" + name;
            CloseableHttpResponse createResponse = client.execute(new HttpPut(indexUri));
            expect200orFail(createResponse, "Failed to create index");
            HttpPut addMapping = new HttpPut(indexUri + "/_mapping/" + type);
            String mappingFilePath = pathToElasticsearchResourceFile(mappingFileName);
            addMapping.setEntity(new FileEntity(new File(mappingFilePath), ContentType.APPLICATION_JSON));
            CloseableHttpResponse mappingResponse = client.execute(addMapping);
            expect200orFail(mappingResponse, "Failed to add mapping to new index");
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create requested index and/or mapping");
        }
    }

    /**
     * Resets the test Elasticsearch instance by deleting all indexes and re-initializing it. This is a somewhat
     * expensive process in terms of time, so it's better that tests use random data and avoid conflicts. Older tests
     * weren't written that way, though.
     */
    public void reset() {
        long start = System.currentTimeMillis();
        try {
            HttpDelete delete = new HttpDelete("http://localhost:9200/_all");
            CloseableHttpResponse deleteResponse = client.execute(delete);
            expect200orFail(deleteResponse, "Couldn't delete indexes from test Elasticsearch");
        } catch (IOException e) {
            throw new IllegalStateException("Couldn't delete indexes from test Elasticsearch", e);
        }
        initIt();
        long elapsed = System.currentTimeMillis() - start;
        StackTraceElement callerElement = Thread.currentThread().getStackTrace()[2];
        log.info("Elasticsearch reset took {} ms; called by {}.{}:{}", new Object[] {
                elapsed, callerElement.getClassName(), callerElement.getMethodName(), callerElement.getLineNumber()});
    }

    private void expect200orFail(CloseableHttpResponse deleteResponse, String message) throws IOException {
        String body = EntityUtils.toString(deleteResponse.getEntity());
        EntityUtils.consume(deleteResponse.getEntity());
        if (deleteResponse.getStatusLine().getStatusCode() != 200) {
            throw new IllegalStateException(message + ": " + body);
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
        // It seems that this library doesn't actually start Elasticsearch until you do something with it, so...
        esSetup.execute(EsSetup.createIndex("init_me"));
        // While this was built for test containers, we can init the tlrx instance the same way.
        initIt();
    }

    /**
     * Initializes the test Elasticsearch server by creating all the normal Blueflood indexes. Uses the
     * version-appropriate init-es.sh script from the blueflood-elasticsearch module, based on the setting of
     * IT_ELASTICSEARCH_CONTAINER_VERSION.
     */
    private void initIt() {
        try {
            String initScript = pathToElasticsearchResourceFile("init-es.sh");
            String command = initScript + " -u localhost:9200";
            log.info("Initialize Elasticsearch for tests with '" + command + "'");
            Process process = Runtime.getRuntime().exec(command);
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            IOUtils.copy(process.getInputStream(), output);
            int exit = process.waitFor();
            if (exit != 0) {
                throw new IllegalStateException("Elasticsearch init script exited with non-zero status; exit=" + exit +
                        "; stdout:\n" + output);
            }
        } catch (IOException | InterruptedException e) {
            throw new IllegalStateException("Failed to run Elasticsearch init script", e);
        }
    }

    /**
     * Finds a particular Elasticsearch init file in src/main/resources of the elasticsearch module. It's expected there
     * will be different directories per Elasticsearch version, and each one will have similarly-named files. For
     * example, to initialize Elasticsearch, we use the file `init-es.sh`, and `metrics_mappings.json` contains the
     * mapping for the `metrics_metadata` index, where metric names are stored.
     */
    private String pathToElasticsearchResourceFile(String fileName) {
        final URL thisModuleConfigFile = getClass().getResource("/blueflood.properties");
        if (thisModuleConfigFile == null) {
            throw new IllegalStateException("Expected to find /blueflood.properties in this module");
        }
        final String resourceInThisModule = thisModuleConfigFile.getFile();
        final String thisModuleResourcesDir = FilenameUtils.getFullPath(resourceInThisModule);
        final String esResourcesDir = thisModuleResourcesDir + "../../../blueflood-elasticsearch/src/main/resources/";
        final String versionFolder;
        if (testContainersEsVersion.startsWith("1.")) {
            versionFolder = "";
        } else if (testContainersEsVersion.startsWith("6.")) {
            versionFolder = "init-es-6";
        } else {
            throw new IllegalStateException("I don't know which ES init resource to use for ES version "
                    + testContainersEsVersion);
        }
        final String finalDir = FilenameUtils.concat(esResourcesDir, versionFolder);
        String path = FilenameUtils.concat(finalDir, fileName);
        if (!new File(path).exists()) {
            throw new IllegalStateException("File doesn't exist at expected path: " + path);
        }
        return path;
    }

    /**
     * Stops the in-memory Elasticsearch managed by this object. It's expected, though unproven, that this would release
     * all resources in use by that Elasticsearch.
     */
    public void stop() {
        try {
            client.close();
        } catch (IOException e) {
            log.warn("Failed to close the HttpClient", e);
        }
        if (esInitMethod.equals("TEST_CONTAINERS")) {
            elasticsearchContainer.stop();
            elasticsearchContainer = null;
        } else if (esInitMethod.equals("TLRX")) {
            esSetup.terminate();
            esSetup = null;
        } else if (esInitMethod.equals("EXTERNAL")) {
            // Do nothing! You have to manage Elasticsearch your own self!
            log.info("Done with external Elasticsearch");
        } else {
            throw new IllegalStateException("Illegal value set for Elasticsearch init in tests: " + esInitMethod);
        }
    }
}
