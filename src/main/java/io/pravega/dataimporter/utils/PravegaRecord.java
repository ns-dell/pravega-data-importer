package io.pravega.dataimporter.utils;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.io.Serializable;
import java.util.HashMap;

public class PravegaRecord implements Serializable {

    private final byte[] key;
    private final byte[] value;
    private final HashMap<String, byte[]> headers = new HashMap<>();
    private final int partition;

    public PravegaRecord(byte[] key, byte[] value, Headers headers, int partition) {
        this.key = key;
        this.value = value;
        this.partition = partition;
        for (Header header: headers){
            this.headers.put(header.key(), header.value());
        }
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
