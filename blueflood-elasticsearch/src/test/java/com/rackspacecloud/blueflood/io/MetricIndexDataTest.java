package com.rackspacecloud.blueflood.io;

import org.elasticsearch.common.lang3.StringUtils;
import org.junit.Test;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class MetricIndexDataTest {

    //regex used by the current analyzer(in index_settings.json) to create metric indexes
    private static final Pattern patternToCreateMetricIndices = Pattern.compile("([^.]+)");
    private static final int METRIC_VALUE = 1;
    @Test
    public void testTokenPathsWithNextLevelSingleIndex() {
        MetricIndexData mi = new MetricIndexData(2);
        mi.add("a.b.c.d", METRIC_VALUE);

        assertEquals("tokens with next level", 1, mi.getTokenPathsWithNextLevel().size());
        assertTrue("token value", mi.getTokenPathsWithNextLevel().contains("a.b.c"));
        assertEquals("base level tokens", 0, mi.getCompleteMetricNamesAtBaseLevel().size());
        assertEquals("next level tokens", 0 , mi.getCompleteMetricNamesAtBasePlusOneLevel().size());
    }

    @Test
    public void testLevel0CompleteMetricNameSingleIndex() {
        MetricIndexData mi = new MetricIndexData(2);
        mi.add("a.b", METRIC_VALUE);

        assertEquals("tokens with next level", 0, mi.getTokenPathsWithNextLevel().size());
        assertEquals("base level tokens", 1, mi.getCompleteMetricNamesAtBaseLevel().size());
        assertEquals("next level tokens", 0 , mi.getCompleteMetricNamesAtBasePlusOneLevel().size());
    }

    @Test
    public void testLevel1CompleteMetricNameSingleIndex() {
        MetricIndexData mi = new MetricIndexData(2);
        mi.add("a.b.c", METRIC_VALUE);

        assertEquals("tokens with next level", 0, mi.getTokenPathsWithNextLevel().size());
        assertEquals("base level tokens", 0, mi.getCompleteMetricNamesAtBaseLevel().size());
        assertEquals("next level tokens", 1, mi.getCompleteMetricNamesAtBasePlusOneLevel().size());
    }

    @Test
    public void testNoCompleteMetricName() {

        ArrayList<String> metricNames = new ArrayList<String>() {{
            add("foo.bar.baz.qux");
        }};
        String query = "foo.bar.*";
        Map<String, Long> metricIndexes = buildMetricIndexesSimilarToES(metricNames, query);

        //for base at foo
        MetricIndexData mi = new MetricIndexData(query.split(AbstractElasticIO.REGEX_TOKEN_DELIMTER).length - 1);
        for (Map.Entry<String, Long> entry: metricIndexes.entrySet()) {
            mi.add(entry.getKey(), entry.getValue());
        }

        assertEquals("Tokens with next level count", 1, mi.getTokenPathsWithNextLevel().size());
        assertEquals("Tokens with next level", true, mi.getTokenPathsWithNextLevel().contains("foo.bar.baz"));
        assertEquals("base level complete metric names count", 0, mi.getCompleteMetricNamesAtBaseLevel().size());
        assertEquals("next level complete metric names count", 0, mi.getCompleteMetricNamesAtBasePlusOneLevel().size());
    }

    @Test
    public void testBaseLevelCompleteMetricName() {

        ArrayList<String> metricNames = new ArrayList<String>() {{
            add("foo.bar.baz.qux.xxx");
            add("foo.bar");
        }};
        String query = "foo.bar.*";
        Map<String, Long> metricIndexes = buildMetricIndexesSimilarToES(metricNames, query);

        //for base at foo.bar
        MetricIndexData mi = new MetricIndexData(query.split(AbstractElasticIO.REGEX_TOKEN_DELIMTER).length - 1);
        for (Map.Entry<String, Long> entry: metricIndexes.entrySet()) {
            mi.add(entry.getKey(), entry.getValue());
        }

        assertEquals("base level complete metric names count", 1, mi.getCompleteMetricNamesAtBaseLevel().size());
        assertEquals("next level complete metric names count", 0, mi.getCompleteMetricNamesAtBasePlusOneLevel().size());
    }


    @Test
    public void testNextLevelCompleteMetricName() {

        ArrayList<String> metricNames = new ArrayList<String>() {{
            add("foo.bar.baz.qux");
            add("foo.bar.baz");
        }};
        String query = "foo.bar.*";
        Map<String, Long> metricIndexes = buildMetricIndexesSimilarToES(metricNames, query);

        //for base at foo
        MetricIndexData mi = new MetricIndexData(query.split(AbstractElasticIO.REGEX_TOKEN_DELIMTER).length - 1);
        for (Map.Entry<String, Long> entry: metricIndexes.entrySet()) {
            mi.add(entry.getKey(), entry.getValue());
        }

        assertEquals("base level complete metric names count", 0, mi.getCompleteMetricNamesAtBaseLevel().size());
        assertEquals("next level complete metric names count", 1, mi.getCompleteMetricNamesAtBasePlusOneLevel().size());
    }

    @Test
    public void testSingleLevelQuery() {

        ArrayList<String> metricNames = new ArrayList<String>() {{
            add("foo.bar.baz");
            add("foo.bar");
        }};
        String query = "*";
        Map<String, Long> metricIndexes = buildMetricIndexesSimilarToES(metricNames, query);

        //for base at foo
        MetricIndexData mi = new MetricIndexData(query.split(AbstractElasticIO.REGEX_TOKEN_DELIMTER).length - 1);
        for (Map.Entry<String, Long> entry: metricIndexes.entrySet()) {
            mi.add(entry.getKey(), entry.getValue());
        }

        assertEquals("Tokens with next level count", 1, mi.getTokenPathsWithNextLevel().size());
        assertEquals("Tokens with next level", true, mi.getTokenPathsWithNextLevel().contains("foo"));
        assertEquals("base level complete metric names count", 0, mi.getCompleteMetricNamesAtBaseLevel().size());
        assertEquals("next level complete metric names count", 0, mi.getCompleteMetricNamesAtBasePlusOneLevel().size());
    }

    @Test
    public void testTwoLevelQuery() {

        ArrayList<String> metricNames = new ArrayList<String>() {{
            add("foo.bar.baz");
            add("foo.bar");
        }};
        String query = "foo.*";
        Map<String, Long> metricIndexes = buildMetricIndexesSimilarToES(metricNames, query);

        //for base at foo
        MetricIndexData mi = new MetricIndexData(query.split(AbstractElasticIO.REGEX_TOKEN_DELIMTER).length - 1);
        for (Map.Entry<String, Long> entry: metricIndexes.entrySet()) {
            mi.add(entry.getKey(), entry.getValue());
        }

        assertEquals("Tokens with next level count", 1, mi.getTokenPathsWithNextLevel().size());
        assertEquals("Tokens with next level", true, mi.getTokenPathsWithNextLevel().contains("foo.bar"));
        assertEquals("base level complete metric names count", 0, mi.getCompleteMetricNamesAtBaseLevel().size());
        assertEquals("next level complete metric names count", 1, mi.getCompleteMetricNamesAtBasePlusOneLevel().size());
    }

    @Test
    public void testMetricIndexesBuilderSingleMetricName() {
        ArrayList<String> metricNames = new ArrayList<String>() {{
            add("foo.bar.baz");
        }};
        Map<String, Long> metricIndexMap = buildMetricIndexesSimilarToES(metricNames, "foo");

        Set<String> expectedIndexes = new HashSet<String>() {{
            add("foo.bar|1");
        }};

        verifyMetricIndexes(metricIndexMap, expectedIndexes);
    }

    @Test
    public void testMetricIndexesBuilderSingleMetricNameSecondTokenQuery() {
        ArrayList<String> metricNames = new ArrayList<String>() {{
            add("foo.bar.baz.qux");
        }};
        Map<String, Long> metricIndexMap = buildMetricIndexesSimilarToES(metricNames, "foo.*");

        Set<String> expectedIndexes = new HashSet<String>() {{
            add("foo.bar|1");
            add("foo.bar.baz|1");
        }};

        verifyMetricIndexes(metricIndexMap, expectedIndexes);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testMetricIndexesBuilderSingleMetricName1() {
        ArrayList<String> metricNames = new ArrayList<String>() {{
            add("foo");
        }};
        buildMetricIndexesSimilarToES(metricNames, "");

    }

    @Test
    public void testMetricIndexesBuilderLongMetrics() {
        ArrayList<String> metricNames = new ArrayList<String>() {{
            add("foo.bar.baz.qux.x.y");
        }};
        Map<String, Long> metricIndexMap = buildMetricIndexesSimilarToES(metricNames, "foo.bar");

        Set<String> expectedIndexes = new HashSet<String>() {{
            add("foo.bar|1");
            add("foo.bar.baz|1");
        }};

        verifyMetricIndexes(metricIndexMap, expectedIndexes);
    }

    @Test
    public void testMetricIndexesBuilderLongMetricsWildcard() {
        ArrayList<String> metricNames = new ArrayList<String>() {{
            add("foo.bar.baz.qux.x.y");
        }};
        Map<String, Long> metricIndexMap = buildMetricIndexesSimilarToES(metricNames, "foo.bar.*");

        Set<String> expectedIndexes = new HashSet<String>() {{
            add("foo.bar|1");
            add("foo.bar.baz|1");
            add("foo.bar.baz.qux|1");
        }};

        verifyMetricIndexes(metricIndexMap, expectedIndexes);
    }

    @Test
    public void testMetricIndexesBuilderMultipleMetrics() {
        ArrayList<String> metricNames = new ArrayList<String>() {{
            add("foo.bar.baz.qux.x");
            add("foo.bar.baz.qux");
            add("foo.bar.baz");
        }};
        Map<String, Long> metricIndexMap = buildMetricIndexesSimilarToES(metricNames, "foo.bar.*");

        Set<String> expectedIndexes = new HashSet<String>() {{
            add("foo.bar|3");
            add("foo.bar.baz|3");
            add("foo.bar.baz.qux|2");
        }};

        verifyMetricIndexes(metricIndexMap, expectedIndexes);
    }


    @Test
    public void testSingleLevelQueryMultipleMetrics() {
        ArrayList<String> metricNames = new ArrayList<String>() {{
            add("foo.bar.baz.x.y.z");
            add("foo.bar");
        }};
        Map<String, Long> metricIndexMap = buildMetricIndexesSimilarToES(metricNames, "foo");

        Set<String> expectedIndexes = new HashSet<String>() {{
            add("foo.bar|2");
        }};

        verifyMetricIndexes(metricIndexMap, expectedIndexes);
    }

    @Test
    public void testSingleLevelWildcardQueryMultipleMetrics() {
        ArrayList<String> metricNames = new ArrayList<String>() {{
            add("foo.bar.baz.x.y.z");
            add("moo.bar");
        }};
        Map<String, Long> metricIndexMap = buildMetricIndexesSimilarToES(metricNames, "*");

        Set<String> expectedIndexes = new HashSet<String>() {{
            add("moo.bar|1");
            add("foo.bar|1");
        }};

        verifyMetricIndexes(metricIndexMap, expectedIndexes);
    }

    private void verifyMetricIndexes(Map<String, Long> metricIndexMap, Set<String> expectedIndexes) {
        Set<String> outputIndexes = new HashSet<String>();
        for (Map.Entry<String, Long> entry: metricIndexMap.entrySet()) {
            outputIndexes.add(entry.getKey() + "|" + entry.getValue());
        }

        assertTrue("All expected indexes should be created", outputIndexes.containsAll(expectedIndexes));
        assertTrue("Output indexes should not exceed expected indexes", expectedIndexes.containsAll(outputIndexes));
    }

    /**
     *
     * This is test method to generate response similar to the ES aggregate search call, where given a
     * metric name regex we get their metric indexes and their aggregate counts.
     *
     * Ex: For a metric foo.bar.baz.qux, it generates a map
     *          {foo, 1}
     *          {bar, 1}
     *          {baz, 1}
     *          {foo.bar, 1}
     *          {foo.bar.baz, 1}
     *
     * For multiple metric names, it generates the corresponding indexes and their document counts.
     *
     * This methods uses the tokenizer and filter similar to the one used during ES setup(index_settings.json).
     *
     * @param metricNames
     * @return
     */
    private Map<String, Long> buildMetricIndexesSimilarToES(List<String> metricNames, String query) {
        Map<String, Long> metricIndexMap = new HashMap<String, Long>();

        for (String metricName: metricNames) {

            int totalTokens = metricName.split(AbstractElasticIO.REGEX_TOKEN_DELIMTER).length;

            //imitating path hierarchy tokenizer(prefix-test-tokenizer) in analyzer(prefix-test-analyzer) we use.
            // For metric, foo.bar.baz path hierarchy tokenizer creates foo, foo.bar, foo.bar.baz

            Set<String> metricIndexes = new HashSet<String>();
            metricIndexes.add(metricName);
            for (int i = 1; i < totalTokens; i++) {
                String path = metricName.substring(0, StringUtils.ordinalIndexOf(metricName, ".", i));
                metricIndexes.add(path);
            }

            //imitating filter(dotted) in the analyzer we use. if path is foo.bar.baz, creates tokens foo, bar, baz
            Set<String> tokens = new HashSet<String>();
            for (String path: metricIndexes) {
                Matcher matcher = patternToCreateMetricIndices.matcher(path);

                while (matcher.find()) {
                    tokens.add(matcher.group(1));
                }
            }
            metricIndexes.addAll(tokens);

            for (String metricIndex: metricIndexes) {
                Long count =  metricIndexMap.containsKey(metricIndex) ? metricIndexMap.get(metricIndex) : 0;
                metricIndexMap.put(metricIndex, count + 1);
            }
        }

        EnumElasticIO enumElasticIO = new EnumElasticIO();
        Pattern patternToGet2Levels = Pattern.compile(enumElasticIO.regexForPrevToNextLevel(query));

        Map<String, Long> outputMap = new HashMap<String, Long>();
        for (Map.Entry<String, Long> entry: metricIndexMap.entrySet()) {
            Matcher matcher = patternToGet2Levels.matcher(entry.getKey());
            if (matcher.matches()) {
                outputMap.put(entry.getKey(), entry.getValue());
            }
        }

        return outputMap;
    }
}
