package com.rackspacecloud.blueflood.io;

import java.util.*;

public class MetricTokenListBuilder {

    /**
     * Given a metric name foo.bar.baz.qux, for query foo.bar.*
     *
     * tokenPathWithNextLevelSet will accumulate all token paths which have next level.
     *      tokenPathWithNextLevelSet  -> {foo.bar.baz, false}
     *
     * metricNameWithEnumExtensionSet will accumulate (metric + enum) matching given query.
     *   if foo.bar is an enum metric in itself with enum values of say [one, two]
     *      metricNameWithEnumExtensionSet      -> {foo.bar.one, true}, {foo.bar.two, true}
     *
     * metricNameWithEnumAsNextLevelMap will accumulate all complete metric names matching query
     *   1) enum metrics:  foo.bar.test with enum values of say [ev1, ev2]
     *      metricNameWithEnumAsNextLevelMap  -> {foo.bar.test, false}
     *
     *   2) regular metric: foo.bar.test with no enum values
     *      metricNameWithEnumAsNextLevelMap  -> {foo.bar.test, true}
     */

    private final Set<MetricToken> tokenPathWithNextLevelSet = new LinkedHashSet<MetricToken>();
    private final Set<MetricToken> metricNameWithEnumExtensionSet = new LinkedHashSet<MetricToken>();
    private final Map<String, Boolean> metricNameWithEnumAsNextLevelMap = new LinkedHashMap<String, Boolean>();


    public MetricTokenListBuilder addTokenPathWithNextLevel(String tokenPath) {
        tokenPathWithNextLevelSet.add(new MetricToken(tokenPath, false));
        return this;
    }

    public MetricTokenListBuilder addTokenPathWithNextLevel(Set<String> tokenPaths) {
        for (String token: tokenPaths) {
            addTokenPathWithNextLevel(token);
        }
        return this;
    }

    public MetricTokenListBuilder addMetricNameWithEnumExtension(String metricTokenPath) {
        metricNameWithEnumExtensionSet.add(new MetricToken(metricTokenPath, true));
        return this;
    }

    public MetricTokenListBuilder addTokenPath(String metricTokenPath, Boolean isLeaf) {
        metricNameWithEnumAsNextLevelMap.put(metricTokenPath, isLeaf);
        return this;
    }

    public ArrayList<MetricToken> build() {

        final ArrayList<MetricToken> resultList = new ArrayList<MetricToken>();
        resultList.addAll(tokenPathWithNextLevelSet);

        for (Map.Entry<String, Boolean> entry : metricNameWithEnumAsNextLevelMap.entrySet()) {
            resultList.add(new MetricToken(entry.getKey(), entry.getValue()));
        }

        resultList.addAll(metricNameWithEnumExtensionSet);

        return resultList;
    }
}