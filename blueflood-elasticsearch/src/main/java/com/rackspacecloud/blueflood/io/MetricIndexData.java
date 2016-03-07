package com.rackspacecloud.blueflood.io;

import java.util.*;

/**
 * When we ingest these below metrics, ES generates the following indexes
 *
 *     metric           indexes
 *   -----------        -------
 *   foo.bar.baz.aux -> [foo, foo.bar, foo.bar.baz, foo.bar.baz.aux, bar, baz, aux]
 *   foo.bar.baz     -> [foo, foo.bar, foo.bar.baz, bar, baz]
 *
 * If we request ES to return us upto next two levels of the metric indexes for foo.bar,
 * we get this below response from ES where key is the metric index and doc_count is
 * the number of documents(think metric names) it points to.
 *
 * "buckets" : [ {
 *     "key" : "foo.bar",
 *     "doc_count" : 2
 *   }, {
 *     "key" : "foo.bar.baz",
 *     "doc_count" : 2
 *   }, {
 *     "key" : "foo.bar.baz.aux",
 *     "doc_count" : 1
 * } ]
 *
 * If we feed each metric index to this class by calling the add(metricIndex, docCount) method,
 * this class analyzes the data and provides methods to retrieve different aspects of this data
 * with respect to baseLevel(if set to 2 with above data, uses foo.bar as reference).
 *
 */
public class MetricIndexData {

    private static class MetricIndexDocCount {

        private long actualDocCount;
        private long childrenTotalDocCount;

        public MetricIndexDocCount(long actualDocCount, long childrenTotalDocCount) {
            this.actualDocCount = actualDocCount;
            this.childrenTotalDocCount = childrenTotalDocCount;
        }

        public void setActualDocCount(long actualDocCount) {
            this.actualDocCount = actualDocCount;
        }

        public void addChildrenTotalDocCount(long childrenDocCount) {
            this.childrenTotalDocCount += childrenDocCount;
        }
    }

    private final int baseLevel;

    //for token paths which have two levels after baseLevel, contains token paths upto next level from baseLevel
    private final Set<String> tokenPathWithNextLevelSet = new LinkedHashSet<String>();

    //contains all metric indexes which are of the same length as baseLevel
    final Map<String, MetricIndexDocCount> metricIndexSameLevelMap = new HashMap<String, MetricIndexDocCount>();

    //contains all metric indexes which are more than the length of baseLevel by one.
    final Map<String, MetricIndexDocCount> metricIndexNextLevelMap = new HashMap<String, MetricIndexDocCount>();

    public MetricIndexData(int baseLevel) {
        this.baseLevel = baseLevel;
    }

    /**
     * For a given metricIndex and docCount, classifies the data with respect to baseLevel
     * and stores it accordingly.
     *
     * @param metricIndex
     * @param docCount is document(metric name) count
     */
    public void add(String metricIndex, long docCount) {

        final String[] tokens = metricIndex.split(AbstractElasticIO.REGEX_TOKEN_DELIMTER);

        /**
         *
         * For the ES response shown in class description, for a baseLevel of 2,
         * the data is classified as follows
         *
         * tokenPathWithNextLevelSet    -> {foo.bar.baz}   (Token paths which are at base + 1 level and also have a subsequent level.)
         *
         * Both these maps maintain data in the form {metricIndex, (actualDocCount, childrenTotalDocCount)}
         *
         * metricIndexSameLevelMap   -> {foo.bar -> (2, 2)}     (all indexes which are of same length as baseLevel)
         * metricIndexNextLevelMap   -> {foo.bar.baz -> (2, 1)} (all indexes which are more than the length of baseLevel by one)
         *
         */

        switch (tokens.length - baseLevel) {
            case 2:
                if (baseLevel > 0) {
                    tokenPathWithNextLevelSet.add(metricIndex.substring(0, metricIndex.lastIndexOf(".")));
                } else {
                    tokenPathWithNextLevelSet.add(metricIndex.substring(0, metricIndex.indexOf(".")));
                }


                //For foo.bar.baz.aux, baseLevel=2, we update children doc count of foo.bar.baz
                addChildrenDocCount(metricIndexNextLevelMap,
                        metricIndex.substring(0, metricIndex.lastIndexOf(".")),
                        docCount);
                break;

            case 1:
                setActualDocCount(metricIndexNextLevelMap, metricIndex, docCount);

                //For foo.bar.baz, baseLevel=2 we update children doc count of foo.bar
                addChildrenDocCount(metricIndexSameLevelMap,
                        metricIndex.substring(0, metricIndex.lastIndexOf(".")),
                        docCount);
                break;

            case 0:

                setActualDocCount(metricIndexSameLevelMap, metricIndex, docCount);
                break;

            default:
                break;
        }
    }

    /**
     * Token paths which are at base + 1 level and also have a subsequent level.
     *
     * Ex: For metrics foo.bar.baz.qux, foo.bar.baz
     *      if baseLevel = 2, returns foo.bar.baz
     *
     * @return
     */
    public Set<String> getTokenPathsWithNextLevel() {
        return Collections.unmodifiableSet(tokenPathWithNextLevelSet);
    }

    /**
     * Returns complete metric names which are of same length as baseLevel
     *
     * Ex: For metrics foo.bar.baz.qux, foo.bar.baz
     *      if baseLevel = 3, returns foo.bar.baz
     *
     * @return
     */
    public Set<String> getCompleteMetricNamesAtBaseLevel() {
        return getCompleteMetricNames(metricIndexSameLevelMap);
    }

    /**
     * Returns complete metric names which are longer than baseLevel by one.
     *
     * Ex: For metrics foo.bar.baz.qux, foo.bar.baz
     *      if baseLevel = 3, returns foo.bar.baz.qux
     *
     * @return
     */
    public Set<String> getCompleteMetricNamesAtBasePlusOneLevel() {
        return getCompleteMetricNames(metricIndexNextLevelMap);
    }

    /**
     *  Compares actualDocCount and total docCount of its immediate children of an index
     *  to determine if the metric index is a complete metric name or not.
     *
     *  For the ES response shown in class description, for a baseLevel of 2,
     *  foo.bar.baz has actualDocCount of 2, but total doc count of all its children,
     *  which in this case is only foo.bar.baz, is 1. So foo.bar.baz must be metric by itself
     *
     * @param metricIndexMap
     * @return
     */
    private Set<String> getCompleteMetricNames(Map<String, MetricIndexDocCount> metricIndexMap) {

        Set<String> completeMetricNames = new HashSet<String>();
        for (Map.Entry<String, MetricIndexDocCount> entry : metricIndexMap.entrySet()) {

            MetricIndexDocCount metricIndexDocCount = entry.getValue();
            if (metricIndexDocCount != null) {
                
                //if total doc count is greater than its children docs, its a complete metric name
                if (metricIndexDocCount.actualDocCount > 0 &&
                        metricIndexDocCount.actualDocCount > metricIndexDocCount.childrenTotalDocCount) {
                    completeMetricNames.add(entry.getKey());
                }
            }
        }

        return Collections.unmodifiableSet(completeMetricNames);
    }

    private void setActualDocCount(Map<String, MetricIndexDocCount> metricIndexMap, String key, final long count) {
        MetricIndexDocCount metricIndexDocCount = metricIndexMap.containsKey(key) ? metricIndexMap.get(key) : new MetricIndexDocCount(0, 0);
        metricIndexDocCount.setActualDocCount(count);

        metricIndexMap.put(key, metricIndexDocCount);
    }

    private void addChildrenDocCount(Map<String, MetricIndexDocCount> metricIndexMap, String key, final long childrenDocCount) {
        MetricIndexDocCount metricIndexDocCount = metricIndexMap.containsKey(key) ? metricIndexMap.get(key) : new MetricIndexDocCount(0, 0);
        metricIndexDocCount.addChildrenTotalDocCount(childrenDocCount);

        metricIndexMap.put(key, metricIndexDocCount);
    }
}
