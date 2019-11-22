package com.vladykin.replicamap.kafka.impl.tx;

import com.vladykin.replicamap.ReplicaMap;
import com.vladykin.replicamap.ReplicaMapException;
import com.vladykin.replicamap.ReplicaMapListener;
import com.vladykin.replicamap.ReplicaMapManager;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

public class TxMap<K,V> extends AbstractMap<K,V> implements ReplicaMap<K,V> {
    protected final TxMapManager manager;
    protected final Map<K,V> tx = new HashMap<>();
    protected final ReplicaMap<K,V> map;

    public TxMap(TxMapManager manager, ReplicaMap<K,V> map) {
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

    @Override
    public Object id() {
        return map.id();
    }

    @Override
    public ReplicaMapManager getManager() {
        return manager;
    }

    @Override
    public Map<K,V> unwrap() {
        return map.unwrap();
    }

    @Override
    public CompletableFuture<V> asyncPut(K key, V value) {
        throw new ReplicaMapException("Async API is not supported within a transaction.");
    }

    @Override
    public CompletableFuture<V> asyncPutIfAbsent(K key, V value) {
        throw new ReplicaMapException("Async API is not supported within a transaction.");
    }

    @Override
    public CompletableFuture<V> asyncReplace(K key, V value) {
        throw new ReplicaMapException("Async API is not supported within a transaction.");
    }

    @Override
    public CompletableFuture<Boolean> asyncReplace(K key, V oldValue, V newValue) {
        throw new ReplicaMapException("Async API is not supported within a transaction.");
    }

    @Override
    public CompletableFuture<V> asyncRemove(K key) {
        throw new ReplicaMapException("Async API is not supported within a transaction.");
    }

    @Override
    public CompletableFuture<Boolean> asyncRemove(K key, V value) {
        throw new ReplicaMapException("Async API is not supported within a transaction.");
    }

    @Override
    public CompletableFuture<V> asyncCompute(K key, BiFunction<? super K,? super V,? extends V> remappingFunction) {
        throw new ReplicaMapException("Async API is not supported within a transaction.");
    }

    @Override
    public CompletableFuture<V> asyncComputeIfPresent(K key,
        BiFunction<? super K,? super V,? extends V> remappingFunction) {
        throw new ReplicaMapException("Async API is not supported within a transaction.");
    }

    @Override
    public CompletableFuture<V> asyncMerge(K key, V value,
        BiFunction<? super V,? super V,? extends V> remappingFunction) {
        throw new ReplicaMapException("Async API is not supported within a transaction.");
    }

    @Override
    public ReplicaMapListener<K,V> getListener() {
        return map.getListener();
    }

    @Override
    public void setListener(ReplicaMapListener<K,V> listener) {
        throw new ReplicaMapException("Can not change listener within a transaction.");
    }

    @Override
    public boolean casListener(ReplicaMapListener<K,V> expected, ReplicaMapListener<K,V> newListener) {
        throw new ReplicaMapException("Can not change listener within a transaction.");
    }
}
