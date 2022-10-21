package io.pravega.dataimporter.utils;

import java.io.Serializable;
import java.util.HashMap;

public class PravegaRecord implements Serializable {

    private final byte[] key;
    private final byte[] value;
    private final HashMap<String, byte[]> headers;
    private final int partition;
    private final String topic;
    private final long timestamp;

    public PravegaRecord(byte[] key, byte[] value, HashMap<String, byte[]> headers, int partition, String topic, long timestamp) {
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.headers = headers;
        this.topic = topic;
        this.timestamp = timestamp;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public HashMap<String, byte[]> getHeaders() {
        return headers;
    }

    public int getPartition() {
        return partition;
    }

    public String getTopic() {
        return topic;
    }

    public long getTimestamp() {
        return timestamp;
    }

}
