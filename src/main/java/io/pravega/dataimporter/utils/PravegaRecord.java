package io.pravega.dataimporter.utils;

import org.apache.kafka.common.header.Headers;

import java.io.Serializable;

public class PravegaRecord implements Serializable {

    private final byte[] key;
    private final byte[] value;
    private final Headers headers;
    private final int partition;

    public PravegaRecord(byte[] key, byte[] value, Headers headers, int partition) {
        this.key = key;
        this.value = value;
        this.headers = headers;
        this.partition = partition;
    }

    public byte[] getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public Headers getHeaders() {
        return headers;
    }

    public int getPartition() {
        return partition;
    }
}
