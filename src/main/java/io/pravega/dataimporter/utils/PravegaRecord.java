/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.dataimporter.utils;

import java.io.Serializable;
import java.util.HashMap;

/**
 * A class containing data extracted from Apache Kafka's {@link org.apache.kafka.clients.consumer.ConsumerRecord}
 * that can be useful when forwarding to a Pravega stream.
 */
public class PravegaRecord implements Serializable {

    private final byte[] key;
    private final byte[] value;
    private final HashMap<String, byte[]> headers;
    private final int partition;
    private final String topic;
    private final long timestamp;

    /**
     * Creates a new instance of the PravegaRecord class.
     */
    public PravegaRecord(byte[] key, byte[] value, HashMap<String, byte[]> headers, int partition, String topic,
                         long timestamp) {
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
