package com.vladykin.replicamap.tx;

import com.vladykin.replicamap.ReplicaMapManager;
import java.util.function.BiFunction;

public interface TxFunction extends BiFunction<ReplicaMapManager, TxMeta, Boolean> {
    @Override
    Boolean apply(ReplicaMapManager txMapManager, TxMeta txMeta);
}
