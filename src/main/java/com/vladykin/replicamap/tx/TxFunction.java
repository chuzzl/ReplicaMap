package com.vladykin.replicamap.tx;

import java.util.function.BiFunction;

/**
 * Transaction logic must be implemented here.
 *
 * Note that this function can be rerun multiple times and it has to have no side effects
 * other than updating the maps provided by {@link TxMaps}.
 */
public interface TxFunction<R> extends BiFunction<TxMaps, TxMeta, R> {
    /**
     * Update maps in transaction.
     *
     * @param txMaps Transactional maps.
     * @param txMeta Metadata for current transaction.
     * @return Result of the transaction logic.
     */
    @Override
    R apply(TxMaps txMaps, TxMeta txMeta);
}
