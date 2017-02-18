package com.rackspacecloud.blueflood.types;

import java.util.Arrays;

import static java.util.stream.Collectors.joining;

/**
 *
 * For a given tenantId = '111111', metricName = 'a.b.c.d',
 *
 * level 2 token will have the following information.
 *
 *      token = 'c'
 *      parent = 'a.b'
 *      isLeaf = false
 *      documentId = '111111:a.b.c'
 *      tenantId = '111111'
 *
 * level 3 token which is a leaf node will have the following information
 *
 *      token = 'd'
 *      parent = 'a.b.c'
 *      isLeaf = true
 *      documentId = '111111:a.b.c.d:$'
 *      tenantId = '111111'
 *
 */
public class Token {

    public static final String LEAF_NODE_SUFFIX = ":$";
    public static final String SEPARATOR = ":";
    public static final String REGEX_TOKEN_DELIMTER = "\\.";

    private final String token;
    private final String parent;
    private final boolean isLeaf;

    private final String documentId;
    private final Locator locator;

    public Token(Locator locator, String[] tokens, int level) {

        if (level > tokens.length - 1)
            throw new IllegalArgumentException("Invalid level for the given tokens");

        this.locator = locator;
        this.token = tokens[level];
        this.isLeaf = level == tokens.length - 1;

        String prefix = locator.getTenantId() + Token.SEPARATOR;
        this.parent = joinTokens("", "", tokens, level);

        String suffix = isLeaf ? LEAF_NODE_SUFFIX : "";
        this.documentId = joinTokens(prefix, suffix, tokens, level + 1);
    }

    private String joinTokens(String prefix, String suffix, String[] tokens, int level) {
        return Arrays.stream(tokens)
                     .limit(level)
                     .collect(joining(".", prefix, suffix));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Token token = (Token) o;

        return documentId.equalsIgnoreCase(token.getDocumentId());
    }

    @Override
    public int hashCode() {
        return documentId == null ? 0 : documentId.hashCode();
    }

    public String getDocumentId() {
        return documentId;
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

    @Override
    public String toString() {
        return "Token{" +
                "token='" + token + '\'' +
                ", parent='" + parent + '\'' +
                ", isLeaf=" + isLeaf +
                ", documentId='" + documentId + '\'' +
                ", locator=" + locator +
                '}';
    }
}