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

import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.dataimporter.AppConfiguration;
import io.pravega.dataimporter.utils.ByteArrayDeserializationFormat;
import io.pravega.dataimporter.utils.ByteArraySerializationFormat;
import io.pravega.dataimporter.utils.Filters;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

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

            final String fixedRoutingKey = getConfig().getParams().get("fixedRoutingKey", "");
            log.info("fixedRoutingKey: {}", fixedRoutingKey);

//            final StreamExecutionEnvironment env = initializeFlinkStreaming();

            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", getConfig().getParams().get("bootstrap.servers","localhost:9092"));
            properties.setProperty("zookeeper.connect", getConfig().getParams().get("zookeeper.connect","localhost:2181"));
            properties.setProperty("group.id", "test");
            final FlinkKafkaConsumer<byte[]> flinkKafkaConsumer = new FlinkKafkaConsumer<>("test-input", new ByteArrayDeserializationFormat(), properties);
            final DataStream<byte[]> events = env
                    .addSource(flinkKafkaConsumer)
                    .uid("kafka-consumer")
                    .name("Kafka consumer from " + getConfig().getParams().get("input-topic"));

            final DataStream<byte[]> toOutput = Filters.dynamicByteArrayFilter(events, getConfig().getParams());

            final FlinkPravegaWriter<byte[]> sink = FlinkPravegaWriter.<byte[]>builder()
                    .withPravegaConfig(outputStreamConfig.getPravegaConfig())
                    .forStream(outputStreamConfig.getStream())
                    .withSerializationSchema(new ByteArraySerializationFormat())
                    .withEventRouter(event -> fixedRoutingKey)
                    .withWriterMode(PravegaWriterMode.EXACTLY_ONCE)
                    .build();
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
