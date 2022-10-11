package io.pravega.dataimporter.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.function.Function;

public class ConsumerRecordByteArrayKafkaDeserializationSchema implements KafkaRecordDeserializationSchema<PravegaRecord> {
    private final Function<ConsumerRecord<byte[], byte[]>, PravegaRecord> function = consumerRecord ->
            new PravegaRecord(consumerRecord.key(),
                    consumerRecord.value(),
                    consumerRecord.headers(),
                    consumerRecord.partition());

    @Override
    public TypeInformation<PravegaRecord> getProducedType() {
        return TypeInformation.of(PravegaRecord.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<PravegaRecord> out) {
        out.collect(function.apply(record));
    }
}
