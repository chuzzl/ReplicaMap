package com.vladykin.replicamap.kafka.impl.util;

public final class FakeCloseable implements AutoCloseable {
    public static final FakeCloseable INSTANCE = new FakeCloseable();

    @Override
    public void close() {
        // no-op
    }
}
