package com.vladykin.replicamap.kafka.impl.tx;

import com.vladykin.replicamap.ReplicaMap;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TxMap<K,V> extends AbstractMap<K,V> {
    protected final TxMapsManager manager;
    protected final Map<K,V> tx = new HashMap<>();
    protected final ReplicaMap<K,V> map;

    public TxMap(TxMapsManager manager, ReplicaMap<K,V> map) {
        this.manager = manager;
        this.map = map;
    }

    @Override
    public Set<Entry<K,V>> entrySet() {
        return null;
    }

    @Override
    public V get(Object key) {
        return null;
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        return null;
    }

    @Override
    public boolean remove(Object key, Object value) {
        return false;
    }

    @Override
    public V put(K key, V value) {
        return null;
    }
}
