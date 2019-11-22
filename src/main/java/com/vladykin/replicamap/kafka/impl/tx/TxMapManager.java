package com.vladykin.replicamap.kafka.impl.tx;

import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.ReplicaMapManager;
import com.vladykin.replicamap.kafka.impl.util.FakeCloseable;
import com.vladykin.replicamap.tx.TxFunction;
import java.util.concurrent.CompletableFuture;

public class TxMapManager implements ReplicaMapManager {
    protected final ReplicaMapManager manager;
    protected final TxFunction tx;

    public TxMapManager(ReplicaMapManager manager, TxFunction tx) {
        this.manager = manager;
        this.tx = tx;
    }

    @Override
    public <K, V> ReplicaMap<K,V> getMap(Object mapId) {
        return manager.getMap(mapId);
    }

    @Override
    public <K, V> ReplicaMap<K,V> getMap() {
        return manager.getMap();
    }

    @Override
    public AutoCloseable readTx() {
        return FakeCloseable.INSTANCE;
    }

    @Override
    public void tx(TxFunction tx) {
        throw new ReplicaMapException("Can not execute transaction within a transaction.");
    }

    @Override
    public CompletableFuture<ReplicaMapManager> start() {
        throw new ReplicaMapException("Can not start manager within a transaction.");
    }

    @Override
    public void stop() {
        throw new ReplicaMapException("Can not stop manager within a transaction.");
    }
}
