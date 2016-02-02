package com.rackspacecloud.blueflood.io;

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
    public String toString() {
        return "TokenInfo{" +
                "token='" + token + '\'' +
                ", isNextLevel=" + isNextLevel +
                '}';
    }
}
