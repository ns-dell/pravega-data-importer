package io.pravega.dataimporter.utils;

import org.apache.kafka.common.header.Headers;

import java.io.Serializable;

public class PravegaRecord<K, V> implements Serializable {

    private final K key;
    private final V value;
    private final Headers headers;
    private final int partition;

    public PravegaRecord(K key, V value, Headers headers, int partition) {
        this.key = key;
        this.value = value;
        this.headers = headers;
        this.partition = partition;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public Headers getHeaders() {
        return headers;
    }

    public int getPartition() {
        return partition;
    }
}
