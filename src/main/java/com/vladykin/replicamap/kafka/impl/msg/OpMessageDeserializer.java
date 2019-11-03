package com.vladykin.replicamap.kafka.impl.msg;

import com.vladykin.replicamap.kafka.impl.util.Utils;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.ByteUtils;

import static com.vladykin.replicamap.base.ReplicaMapBase.OP_FLUSH_NOTIFICATION;
import static com.vladykin.replicamap.base.ReplicaMapBase.OP_FLUSH_REQUEST;
import static com.vladykin.replicamap.kafka.impl.msg.OpMessageSerializer.NULL_ARRAY_LENGTH;

public class OpMessageDeserializer<V> implements Deserializer<OpMessage> {
    protected final Deserializer<V> valDes;

    public OpMessageDeserializer(Deserializer<V> valDes) {
        this.valDes = Utils.requireNonNull(valDes, "valDes");
    }

    @Override
    public void configure(Map<String,?> configs, boolean isKey) {
        valDes.configure(configs, isKey);
    }

    protected byte[] readByteArray(ByteBuffer buf) {
        int len = ByteUtils.readVarint(buf);

        if (len == NULL_ARRAY_LENGTH)
            return null;

        byte[] arr = new byte[len];

        if (len != 0)
            buf.get(arr);

        return arr;
    }

    protected V readValue(String topic, Headers headers, ByteBuffer buf) {
        byte[] arr = readByteArray(buf);
        return arr == null ? null :
            valDes.deserialize(topic, headers, arr);
    }

    @Override
    public OpMessage deserialize(String topic, byte[] opMsgBytes) {
        return deserialize(topic, null, opMsgBytes);
    }

    @Override
    public OpMessage deserialize(String topic, Headers headers, byte[] opMsgBytes) {
        ByteBuffer buf = ByteBuffer.wrap(opMsgBytes);
        byte opType = buf.get();

        if (opType == OP_FLUSH_REQUEST || opType == OP_FLUSH_NOTIFICATION) {
            return new OpMessage(opType,
                ByteUtils.readVarlong(buf),
                ByteUtils.readVarlong(buf),
                ByteUtils.readVarlong(buf),
                ByteUtils.readVarlong(buf));
        }

        return new OpMessage(
            opType,
            ByteUtils.readVarlong(buf),
            ByteUtils.readVarlong(buf),
            readValue(topic, headers, buf),
            readValue(topic, headers, buf),
            null // FIXME
        );
    }

    @Override
    public void close() {
        Utils.close(valDes);
    }
}
