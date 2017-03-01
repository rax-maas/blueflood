package com.rackspacecloud.blueflood.types;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 *
 * For a given tenantId = '111111', metricName = 'a.b.c.d',
 *
 * level 2 token will have the following information.
 *
 *      token = 'c'
 *      parent = 'a.b'
 *      isLeaf = false
 *      id = '111111:a.b.c'
 *      tenantId = '111111'
 *
 * level 3 token which is a leaf node will have the following information
 *
 *      token = 'd'
 *      parent = 'a.b.c'
 *      isLeaf = true
 *      id = '111111:a.b.c.d:$'
 *      tenantId = '111111'
 *
 */
public class Token {

    public static final String LEAF_NODE_SUFFIX = ":$";
    public static final String SEPARATOR = ":";

    private final String token;
    private final String parent;
    private final boolean isLeaf;

    private final String id;
    private final Locator locator;

    /**
     * Constructor used to create {@link Token} instances.
     *
     * @param locator is a metric locator
     * @param tokens, array of tokens. For metric name a.b.c.d, tokens would be the array ["a", "b", "c", "d"]
     * @param level level of token this object represents. Ex: level 1 of metric a.b.c.d, would be "b"
     */
    public Token(Locator locator, String[] tokens, int level) {

        if (tokens == null || tokens.length <= 0)
            throw new IllegalArgumentException("Invalid tokens. Must be an array of size " +
                                                       "greater than 0, representing tokens of a metric name.");

        if (level < 0 || level > tokens.length - 1)
            throw new IllegalArgumentException("Invalid level for the given tokens");

        this.locator = locator;
        this.token = tokens[level];
        this.isLeaf = level == tokens.length - 1;

        String prefix = locator.getTenantId() + Token.SEPARATOR;
        this.parent = joinTokens("", "", tokens, level);

        String suffix = isLeaf ? LEAF_NODE_SUFFIX : "";
        this.id = joinTokens(prefix, suffix, tokens, level + 1);
    }

    private String joinTokens(String prefix, String suffix, String[] tokens, int level) {
        return Arrays.stream(tokens)
                     .limit(level)
                     .collect(joining(Locator.METRIC_TOKEN_SEPARATOR, prefix, suffix));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Token token = (Token) o;

        return id.equalsIgnoreCase(token.getId());
    }

    @Override
    public int hashCode() {
        return id == null ? 0 : id.hashCode();
    }

    public String getId() {
        return id;
    }

    public Locator getLocator() {
        return locator;
    }

    public String getToken() {
        return token;
    }

    public String getParent() {
        return parent;
    }

    public boolean isLeaf() {
        return isLeaf;
    }


    /**
     * This method return list of tokens with their parents for a current Discovery object.
     *
     * For example: A locator of 1111:a.b.c.d would generate the following tokens
     *
     * Token{token='a', parent='', isLeaf=false, id='111111:a', locator=111111.a.b.c.d}
     * Token{token='b', parent='a', isLeaf=false, id='111111:a.b', locator=111111.a.b.c.d}
     * Token{token='c', parent='a.b', isLeaf=false, id='111111:a.b.c', locator=111111.a.b.c.d}
     * Token{token='d', parent='a.b.c', isLeaf=true, id='111111:a.b.c.d:$', locator=111111.a.b.c.d}
     *
     * @return
     */
    public static List<Token> getTokens(Locator locator) {

        if (StringUtils.isEmpty(locator.getMetricName()) || StringUtils.isEmpty(locator.getTenantId()))
            return new ArrayList<>();

        String[] tokens = locator.getMetricName().split(Locator.METRIC_TOKEN_SEPARATOR_REGEX);

        return IntStream.range(0, tokens.length)
                        .mapToObj(index -> new Token(locator, tokens, index))
                        .collect(toList());
    }

    public static Stream<Token> getUniqueTokens(Stream<Locator> locators) {
        return locators.flatMap(locator -> Token.getTokens(locator).stream())
                       .collect(toSet())
                       .stream();
    }

    @Override
    public String toString() {
        return "Token{" +
                "token='" + token + '\'' +
                ", parent='" + parent + '\'' +
                ", isLeaf=" + isLeaf +
                ", id='" + id + '\'' +
                ", locator=" + locator +
                '}';
    }
}