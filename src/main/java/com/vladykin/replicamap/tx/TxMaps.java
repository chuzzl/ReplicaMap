package com.vladykin.replicamap.tx;

import com.vladykin.replicamap.ReplicaMapManager;
import java.util.Map;

/**
 * Provides access to maps the same way as {@link ReplicaMapManager} does.
 * The difference is that these maps are not thread safe and must be updated
 * within the current transaction.
 */
public interface TxMaps {
    /**
     * Get the default map instance.
     * May throw an exception if not supported.
     *
     * @return Default map.
     * @see ReplicaMapManager#getMap()
     */
    <K,V> Map<K,V> getMap();

    /**
     * Get the map instance by the given identifier.
     *
     * @param mapId Map identifier.
     * @return Map.
     * @see ReplicaMapManager#getMap(Object)
     */
    <K,V> Map<K,V> getMap(Object mapId);
}
