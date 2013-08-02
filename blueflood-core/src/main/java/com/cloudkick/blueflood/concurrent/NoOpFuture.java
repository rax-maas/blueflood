package com.cloudkick.blueflood.concurrent;

import com.google.common.util.concurrent.AbstractFuture;

public class NoOpFuture<V> extends AbstractFuture<V> {
    public NoOpFuture(V result) {
        set(result);
    }
}
