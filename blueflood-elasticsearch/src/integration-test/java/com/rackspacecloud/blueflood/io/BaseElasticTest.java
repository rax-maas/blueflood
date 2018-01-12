package com.rackspacecloud.blueflood.io;

import com.rackspacecloud.blueflood.types.*;
import com.rackspacecloud.blueflood.utils.TimeValue;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class BaseElasticTest extends ESIntegTestCase {

    protected static final int NUM_PARENT_ELEMENTS = 30;
    protected static final List<String> CHILD_ELEMENTS = Arrays.asList("A", "B", "C");
    protected static final int NUM_GRANDCHILD_ELEMENTS = 3;
    protected static final int NUM_DOCS = NUM_PARENT_ELEMENTS * CHILD_ELEMENTS.size() * NUM_GRANDCHILD_ELEMENTS;
    protected static final String TENANT_A = "ratanasv";
    protected static final String TENANT_B = "someotherguy";
    protected static final String TENANT_C = "someothergal";
    protected static final String UNIT = "horse length";

    protected static final Map<String, List<Locator>> locatorMap = new HashMap<String, List<Locator>>();

    protected SearchResult createExpectedResult(String tenantId, int x, String y, int z, String unit) {
        Locator locator = createTestLocator(tenantId, x, y, z);
        return new SearchResult(tenantId, locator.getMetricName(), unit);
    }

    /**
     *
     * The below code generate locator's which match the below regex.
     *
     *      one\.two\.three[0-2][0-9].four[A-C].five[0-2]
     *
     * Examples:
     *      one.two.three00.fourA.five0
     *      one.two.three29.fourC.five2
     *
     */
    protected Locator createTestLocator(String tenantId, int x, String y, int z) {
        String xs = (x < 10 ? "0" : "") + String.valueOf(x);
        return Locator.createLocatorFromPathComponents(
                tenantId, "one", "two", "three" + xs,
                "four" + y,
                "five" + String.valueOf(z));
    }

    protected List<Locator> createComplexTestLocators(String tenantId) {
        Locator locator;
        List<Locator> locators = new ArrayList<Locator>();
        locatorMap.put(tenantId, locators);
        for (int x = 0; x < NUM_PARENT_ELEMENTS; x++) {
            for (String y : CHILD_ELEMENTS) {
                for (int z = 0; z < NUM_GRANDCHILD_ELEMENTS; z++) {
                    locator = createTestLocator(tenantId, x, y, z);
                    locators.add(locator);
                }
            }
        }
        return locators;
    }

    protected void createTestMetrics(String tenantId, Set<String> fullyQualifiedMetricNames) throws Exception {

        List<IMetric> metrics = new ArrayList<IMetric>();
        for (String metricName: fullyQualifiedMetricNames) {
            metrics.add(new Metric(Locator.createLocatorFromPathComponents(tenantId, metricName),
                    5647382910L, 0, new TimeValue(1, TimeUnit.DAYS), UNIT));
        }

        createTestMetrics(metrics);
    }

    protected void createTestMetrics(List<IMetric> metrics) throws IOException {
        insertDiscovery(metrics);
        refreshChanges();
    }


    protected abstract void insertDiscovery(List<IMetric> metrics) throws IOException;

    protected List<IMetric> createTestMetrics(String tenantId) {
        Metric metric;
        List<IMetric> metrics = new ArrayList<IMetric>();
        List<Locator> locators = createComplexTestLocators(tenantId);
        for (Locator locator : locators) {
            metric = new Metric(locator, 123456789L, 0, new TimeValue(1, TimeUnit.DAYS), UNIT);
            metrics.add(metric);
        }
        return metrics;
    }

    protected List<IMetric> createTestMetricsFromInterface(String tenantId) {
        IMetric metric;
        List<IMetric> metrics = new ArrayList<IMetric>();
        BluefloodCounterRollup counter = new BluefloodCounterRollup();

        List<Locator> locators = createComplexTestLocators(tenantId);
        for (Locator locator : locators) {
            metric = new PreaggregatedMetric(0, locator, new TimeValue(1, TimeUnit.DAYS), counter);
            metrics.add(metric);
        }
        return metrics;
    }

    protected void verifyResults(List<MetricName> results, Set<String> expectedResults) {
        Set<String> formattedResults = formatForComparision(results);
        assertTrue("Expected results does not contain all of api results", expectedResults.containsAll(formattedResults));
        assertTrue("API results does not contain all of expected results", formattedResults.containsAll(expectedResults));
        assertEquals("Invalid total number of results", expectedResults.size(), results.size());
    }

    protected Set<String> formatForComparision(List<MetricName> results) {
        final String DELIMITER = "|";

        Set<String> formattedResults = new HashSet<String>();
        for (MetricName metricName : results) {
            formattedResults.add(metricName.getName() + DELIMITER + metricName.isCompleteName());
        }

        return formattedResults;
    }

    private static String convertStreamToString(java.io.InputStream is) {
        Scanner s = new Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }

    private static String getDataAsString(String fileName) {
        return convertStreamToString(BaseElasticTest.class
                                             .getClassLoader()
                                             .getResourceAsStream(fileName));
    }

    protected static String getIndexSettings() {
        return getDataAsString("index_settings.json");
    }

    protected static String getMetricsMapping() {
        return getDataAsString("metrics_mapping.json");
    }

    protected static String getTokensMapping() {
        return getDataAsString("tokens_mapping.json");
    }

    protected static String getEventsMapping() {
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
}
