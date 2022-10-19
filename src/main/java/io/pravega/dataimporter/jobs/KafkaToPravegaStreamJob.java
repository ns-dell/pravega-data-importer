/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.dataimporter.jobs;

import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.connectors.flink.serialization.PravegaSerializationSchema;
import io.pravega.dataimporter.client.AppConfiguration;
import io.pravega.dataimporter.utils.ConsumerRecordByteArrayKafkaDeserializationSchema;
import io.pravega.dataimporter.utils.PravegaRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

/**
 * Continuously copy a Kafka stream to a Pravega stream.
 */
public class KafkaToPravegaStreamJob extends AbstractJob {
    final private static Logger log = LoggerFactory.getLogger(KafkaToPravegaStreamJob.class);

    final String jobName = getConfig().getJobName(KafkaToPravegaStreamJob.class.getName());

    final StreamExecutionEnvironment env;

    public KafkaToPravegaStreamJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
        env = initializeFlinkStreaming();
    }

    public void run() {
        try {
            final AppConfiguration.StreamConfig outputStreamConfig = getConfig().getStreamConfig("output");
            log.info("output stream: {}", outputStreamConfig);

            final boolean isStreamOrdered = getConfig().getParams().getBoolean("isStreamOrdered", true);
            log.info("isStreamOrdered: {}", isStreamOrdered);

            String bootstrap_servers = getConfig().getParams().get("bootstrap.servers","localhost:9092");
            String kafkaTopic = getConfig().getParams().get("input-topic");
            final KafkaSource<PravegaRecord> kafkaSource = KafkaSource.<PravegaRecord>builder()
                    .setBootstrapServers(bootstrap_servers)
                    .setTopics(Collections.singletonList(kafkaTopic))
                    .setDeserializer(new ConsumerRecordByteArrayKafkaDeserializationSchema())
                    .build();

            final DataStream<PravegaRecord> toOutput =
                    env.fromSource(
                            kafkaSource,
                            WatermarkStrategy.noWatermarks(),
                            "Kafka consumer from " + getConfig().getParams().get("input-topic"));

            final FlinkPravegaWriter<PravegaRecord> sink;
            FlinkPravegaWriter.Builder<PravegaRecord> flinkPravegaWriterBuilder = FlinkPravegaWriter.<PravegaRecord>builder()
                    .withPravegaConfig(outputStreamConfig.getPravegaConfig())
                    .forStream(outputStreamConfig.getStream())
                    .withSerializationSchema(new PravegaSerializationSchema<>(new JavaSerializer<>()));
            if (isStreamOrdered){
                //ordered write, multi-partition. routing key taken from ConsumerRecord key if exists, else ConsumerRecord partition
                flinkPravegaWriterBuilder.withEventRouter(event -> (event.getKey() != null ? Arrays.toString(event.getKey()) : String.valueOf(event.getPartition())));
            }
            flinkPravegaWriterBuilder.withWriterMode(PravegaWriterMode.EXACTLY_ONCE);

            sink = flinkPravegaWriterBuilder.build();
            toOutput
                    .addSink(sink)
                    .uid("pravega-writer")
                    .name("Pravega writer to " + outputStreamConfig.getStream().getScopedName());

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
