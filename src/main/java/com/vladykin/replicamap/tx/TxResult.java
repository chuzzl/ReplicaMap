package com.vladykin.replicamap.tx;

import com.vladykin.replicamap.ReplicaMapManager;

public interface TxResult {
    ReplicaMapManager getManager();
    TxFunction getTx();
}
