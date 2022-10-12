package io.pravega.dataimporter.utils;

import java.io.Serializable;
import java.util.HashMap;

public class PravegaRecord implements Serializable {

    private final byte[] key;
    private final byte[] value;
    private final HashMap<String, byte[]> headers;
    private final int partition;

    public PravegaRecord(byte[] key, byte[] value, HashMap<String, byte[]> headers, int partition) {
        this.key = key;
        this.value = value;
        this.partition = partition;
        this.headers = headers;
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
}
