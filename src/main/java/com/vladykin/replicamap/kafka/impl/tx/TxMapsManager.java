package com.vladykin.replicamap.kafka.impl.tx;

import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.ReplicaMapManager;
import com.vladykin.replicamap.tx.TxFunction;
import com.vladykin.replicamap.tx.TxMaps;

public class TxMapsManager implements TxMaps {
    protected final ReplicaMapManager manager;
    protected final TxFunction tx;

    public TxMapsManager(ReplicaMapManager manager, TxFunction tx) {
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
}
