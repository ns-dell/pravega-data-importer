package io.pravega.dataimporter.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.Function;

public class ConsumerRecordByteArrayKafkaDeserializationSchema implements KafkaDeserializationSchema<PravegaRecord> {
    private final Function<ConsumerRecord<byte[], byte[]>, PravegaRecord> function = consumerRecord ->
            new PravegaRecord(consumerRecord.key(),
                    consumerRecord.value(),
                    consumerRecord.headers(),
                    consumerRecord.partition());

    @Override
    public boolean isEndOfStream(PravegaRecord nextElement) {
        return false;
    }

    @Override
    public PravegaRecord deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return function.apply(record);
    }

    @Override
    public TypeInformation<PravegaRecord> getProducedType() {
        return TypeInformation.of(PravegaRecord.class);
    }
}
