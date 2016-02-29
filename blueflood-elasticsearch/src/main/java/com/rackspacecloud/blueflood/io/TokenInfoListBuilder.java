package com.rackspacecloud.blueflood.io;

import java.util.*;

public class TokenInfoListBuilder {

    /**
     * Given a metric name foo.bar.baz.qux, for prefix foo.bar
     * (prefix foo.bar is considered level 0, baz is level 1)
     *
     * tokensWithNextLevelSet will accumulate all tokens at level 1 which also have subsequent next level
     *      tokensWithNextLevelSet  -> {foo.bar.baz, true}
     *
     * enumValuesAs1LevelSet will accumulate all enum values which are at level 1.
     * if foo.bar is an enum metric in itself with enum values of say [one, two]
     *      enumValuesAs1LevelSet      -> {foo.bar.one, false}, {foo.bar.two, false}
     *
     * tokensWithEnumsAs2LevelMap will accumulate all tokens at level 1 which have enum values.
     * if foo.bar.baz is an enum metric in itself with enum values of say [something]
     *      tokensWithEnumsAs2LevelMap  -> {foo.bar.baz, true}
     *
     * if there is another regular metric say at level 1, say foo.bar.test (no enum values)
     *      tokensWithEnumsAs2LevelMap  -> {foo.bar.baz, true}, {foo.bar.test, false}
     */

    private final Set<TokenInfo> tokensWithNextLevelSet = new LinkedHashSet<TokenInfo>();
    private final Set<TokenInfo> enumValuesAs1LevelSet = new LinkedHashSet<TokenInfo>();
    private final Map<String, Boolean> tokensWithEnumsAs2LevelMap = new LinkedHashMap<String, Boolean>();


    public TokenInfoListBuilder addTokenWithNextLevel(String token) {
        tokensWithNextLevelSet.add(new TokenInfo(token, true));
        return this;
    }

    public TokenInfoListBuilder addTokenWithNextLevel(Set<String> tokens) {
        for (String token: tokens) {
            addTokenWithNextLevel(token);
        }
        return this;
    }

    public TokenInfoListBuilder addEnumValues(String metricName, List<String> enumValues) {
        for (String enumValue: enumValues) {
            enumValuesAs1LevelSet.add(new TokenInfo(metricName + "." + enumValue, false));
        }
        return this;
    }

    public TokenInfoListBuilder addToken(String token, Boolean isNextLevel) {
        tokensWithEnumsAs2LevelMap.put(token, isNextLevel);
        return this;
    }

    public ArrayList<TokenInfo> build() {

        final ArrayList<TokenInfo> resultList = new ArrayList<TokenInfo>();
        resultList.addAll(tokensWithNextLevelSet);

        for (Map.Entry<String, Boolean> entry : tokensWithEnumsAs2LevelMap.entrySet()) {
            resultList.add(new TokenInfo(entry.getKey(), entry.getValue()));
        }

        resultList.addAll(enumValuesAs1LevelSet);

        return resultList;
    }
}