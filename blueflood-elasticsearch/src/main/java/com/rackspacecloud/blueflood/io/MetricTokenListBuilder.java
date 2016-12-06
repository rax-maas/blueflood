package com.rackspacecloud.blueflood.io;

import java.util.*;

public class MetricTokenListBuilder {

    /**
     * Given a metric name foo.bar.baz.qux, for query foo.bar.*
     *
     * tokenPathWithNextLevelSet will accumulate all token paths which have next level.
     *      tokenPathWithNextLevelSet  -> {foo.bar.baz, false}
     *
     * metricNameWithEnumAsNextLevelMap will accumulate all complete metric names matching query
     *      regular metric: foo.bar.test
     *      metricNameWithEnumAsNextLevelMap  -> {foo.bar.test, true}
     */

    private final Set<MetricToken> tokenPathWithNextLevelSet = new LinkedHashSet<MetricToken>();
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

        return resultList;
    }
}