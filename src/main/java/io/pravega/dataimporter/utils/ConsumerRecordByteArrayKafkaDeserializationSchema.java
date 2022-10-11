package io.pravega.dataimporter.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Function;

public class ConsumerRecordByteArrayKafkaDeserializationSchema implements KafkaDeserializationSchema<PravegaRecord<byte[], byte[]>> {

    private final Function<ConsumerRecord<byte[], byte[]>, PravegaRecord<byte[], byte[]>> function = consumerRecord ->
            new PravegaRecord<>(consumerRecord.key(), consumerRecord.value(), consumerRecord.headers(), consumerRecord.partition());

    @Override
    public boolean isEndOfStream(PravegaRecord<byte[], byte[]> nextElement) {
        return false;
    }

    @Override
    public PravegaRecord<byte[], byte[]> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return function.apply(record);
    }

    @Override
    public TypeInformation<PravegaRecord<byte[], byte[]>> getProducedType() {
        return null;
    }
}
