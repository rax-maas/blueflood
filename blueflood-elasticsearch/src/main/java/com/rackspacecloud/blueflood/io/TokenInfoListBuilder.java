package com.rackspacecloud.blueflood.io;

import java.util.*;

public class TokenInfoListBuilder {

    /**
     * Given a metric name foo.bar.baz.qux, for prefix foo.bar
     * (prefix foo.bar is considered level 0, baz is level 1)
     *
     * tokensWithNextLevelMap will accumulate all tokens at level 1 which also have subsequent next level
     *      tokensWithNextLevelMap  -> {baz, true}
     *
     * enumValuesAs1Level1Map will accumulate all enum values which are at level 1.
     * if foo.bar is an enum metric in itself with enum values of say [one, two]
     *      enumValuesAs1Level1Map      -> {one, false}, {two, false}
     *
     * tokensWithEnumsAs2LevelMap will accumulate all tokens at level 1 which have enum values.
     * if foo.bar.baz is an enum metric in itself with enum values of say [something]
     *      tokensWithEnumsAs2LevelMap  -> {baz, true}
     *
     * if there is another regular metric say at level 1, say foo.bar.test (no enum values)
     *      tokensWithEnumsAs2LevelMap  -> {baz, true}, {test, false}
     */

    private final Map<String, Boolean> tokensWithNextLevelMap = new LinkedHashMap<String, Boolean>();
    private final Map<String, Boolean> enumValuesAs1Level1Map = new LinkedHashMap<String, Boolean>();
    private final Map<String, Boolean> tokensWithEnumsAs2LevelMap = new LinkedHashMap<String, Boolean>();


    public TokenInfoListBuilder addTokenWithNextLevel(String token) {
        tokensWithNextLevelMap.put(token, true);
        return this;
    }

    public TokenInfoListBuilder addTokenWithNextLevel(Set<String> tokens) {
        for (String token: tokens) {
            tokensWithNextLevelMap.put(token, true);
        }
        return this;
    }

    public TokenInfoListBuilder addEnumValues(List<String> enumValues) {
        for (String enumValue: enumValues) {
            enumValuesAs1Level1Map.put(enumValue, false);
        }
        return this;
    }

    public TokenInfoListBuilder addToken(String token, Boolean isNextLevel) {
        tokensWithEnumsAs2LevelMap.put(token, isNextLevel);
        return this;
    }

    public ArrayList<TokenInfo> build() {

        final ArrayList<TokenInfo> resultList = new ArrayList<TokenInfo>();
        for (Map.Entry<String, Boolean> entry : tokensWithNextLevelMap.entrySet()) {
            resultList.add(new TokenInfo(entry.getKey(), entry.getValue()));
        }

        for (Map.Entry<String, Boolean> entry : tokensWithEnumsAs2LevelMap.entrySet()) {
            resultList.add(new TokenInfo(entry.getKey(), entry.getValue()));
        }

        for (Map.Entry<String, Boolean> entry : enumValuesAs1Level1Map.entrySet()) {
            resultList.add(new TokenInfo(entry.getKey(), entry.getValue()));
        }

        return resultList;
    }
}
