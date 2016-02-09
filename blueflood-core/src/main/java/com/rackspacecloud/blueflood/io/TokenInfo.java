package com.rackspacecloud.blueflood.io;

import org.apache.commons.lang.builder.HashCodeBuilder;

public final class TokenInfo {

    private final String token;
    private final boolean isNextLevel;

    public TokenInfo(String token, boolean isNextLevel) {
        this.token = token;
        this.isNextLevel = isNextLevel;
    }

    public String getToken() {
        return token;
    }

    public boolean isNextLevel() {
        return isNextLevel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TokenInfo tokenInfo = (TokenInfo) o;

        if (isNextLevel != tokenInfo.isNextLevel) return false;
        return !(token != null ? !token.equals(tokenInfo.token) : tokenInfo.token != null);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(token)
                .append(isNextLevel)
                .toHashCode();
    }

    @Override
    public String toString() {
        return "TokenInfo{" +
                "token='" + token + '\'' +
                ", isNextLevel=" + isNextLevel +
                '}';
    }
}
