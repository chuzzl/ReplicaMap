package com.vladykin.replicamap.kafka.impl.msg;

/**
 * Operation message base class.
 *
 * @author Sergi Vladykin http://vladykin.com
 */
public abstract class OpMessage {
    // List of message types.
    public static final byte OP_PUT = 'p';
    public static final byte OP_PUT_IF_ABSENT = 'P';

    public static final byte OP_REPLACE_ANY = 'c';
    public static final byte OP_REPLACE_EXACT = 'C';

    public static final byte OP_REMOVE_ANY = 'r';
    public static final byte OP_REMOVE_EXACT = 'R';

    public static final byte OP_COMPUTE = 'x';
    public static final byte OP_COMPUTE_IF_PRESENT = 'X';

    public static final byte OP_MERGE = 'm';

    public static final byte OP_FLUSH_REQUEST = 'f';
    public static final byte OP_FLUSH_NOTIFICATION = 'F';

    protected final long clientId;
    protected final byte opType;

    public OpMessage(byte opType, long clientId) {
        this.opType = opType;
        this.clientId = clientId;
    }

    public byte getOpType() {
        return opType;
    }

    public long getClientId() {
        return clientId;
    }
}
