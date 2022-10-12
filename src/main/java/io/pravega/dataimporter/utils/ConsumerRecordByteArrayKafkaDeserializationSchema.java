package io.pravega.dataimporter.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.util.HashMap;

public class ConsumerRecordByteArrayKafkaDeserializationSchema implements KafkaRecordDeserializationSchema<PravegaRecord> {

    @Override
    public TypeInformation<PravegaRecord> getProducedType() {
        return TypeInformation.of(PravegaRecord.class);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<PravegaRecord> out) {
        HashMap<String, byte[]> headers = new HashMap<>();
        for (Header header: record.headers()){
            headers.put(header.key(), header.value());
        }
        out.collect(new PravegaRecord(record.key(), record.value(), headers, record.partition()));
    }
}
