package io.pravega.dataimporter.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ConsumerRecordByteArrayKafkaDeserializationSchema implements KafkaRecordDeserializationSchema<PravegaRecord> {

    @Override
    public TypeInformation<PravegaRecord> getProducedType() {
        return TypeInformation.of(PravegaRecord.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<PravegaRecord> out) {
        out.collect(new PravegaRecord(record.key(), record.value(), record.headers(), record.partition()));
    }
}
