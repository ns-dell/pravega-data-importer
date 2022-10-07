package io.pravega.dataimporter.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerRecordByteArrayKafkaDeserializationSchema implements KafkaDeserializationSchema<ConsumerRecord<byte[], byte[]>> {

    @Override
    public boolean isEndOfStream(ConsumerRecord<byte[], byte[]> nextElement) {
        return false;
    }

    @Override
    public ConsumerRecord<byte[], byte[]> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return record;
    }

    @Override
    public TypeInformation<ConsumerRecord<byte[], byte[]>> getProducedType() {
        return null;
    }
}
