package io.pravega.dataimporter;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.dataimporter.jobs.AbstractJob;
import io.pravega.dataimporter.utils.PravegaRecord;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;
import java.util.Objects;

import static io.pravega.dataimporter.jobs.KafkaMirroringJob.createFlinkPravegaWriterForPravegaRecord;
import static io.pravega.dataimporter.jobs.KafkaMirroringJob.createKafkaSourceForPravegaRecord;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static net.mguenther.kafka.junit.SendValues.to;

public class KafkaMirroringJobTest {

    final private static Logger log = LoggerFactory.getLogger(KafkaMirroringJobTest.class);

    private static final int READER_TIMEOUT_MS = 2000;

    private EmbeddedKafkaCluster kafka;

    @BeforeEach
    void setupKafka() {
        kafka = provisionWith(defaultClusterConfig());
        kafka.start();
    }

    @AfterEach
    void tearDownKafka() {
        kafka.stop();
    }

    @Test
    public void shouldWaitForRecordsToBePublished() throws Exception {
        kafka.send(to("test-topic", "a", "b", "c"));
        Assertions.assertEquals(3, kafka.observe(on("test-topic", 3)).size());
    }

    @Test
    public void testKafkaToPravegaStreamJob() throws Exception {

        kafka.send(to("test-topic",
                new ConsumerRecord<>("test-topic",
                        1,
                        0L,
                        "key1".getBytes(),
                        "value1".getBytes())));

        PravegaTestResource remoteTestResource = new PravegaTestResource(
                9090,
                12345,
                "remoteScope",
                "remoteStream");
        remoteTestResource.start();

        HashMap<String, String> argsMap = new HashMap<>();
        argsMap.put("action-type", "kafka-stream-mirroring");
        argsMap.put("input-topic", "test-topic");
        argsMap.put("output-stream", "remoteScope/remoteStream");
        argsMap.put("output-controller", "tcp://127.0.0.1:9990");
        argsMap.put("bootstrap.servers", "localhost:9092");
        argsMap.put("isStreamOrdered", String.valueOf(true));

        AppConfiguration appConfiguration = new AppConfiguration(argsMap);

        final AppConfiguration.StreamConfig outputStreamConfig = appConfiguration.getStreamConfig("output");
        final String bootstrap_servers = appConfiguration.getParams().get(
                "bootstrap.servers","localhost:9092");
        final String kafkaTopic = appConfiguration.getParams().get("input-topic");

        final KafkaSource<PravegaRecord> kafkaSource =
                createKafkaSourceForPravegaRecord(bootstrap_servers, kafkaTopic);

        StreamExecutionEnvironment testEnvironment = AbstractJob.initializeFlinkStreaming(
                appConfiguration, false);

        final DataStream<PravegaRecord> toOutput =
                testEnvironment.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka consumer from " + appConfiguration.getParams().get("input-topic"));

        final FlinkPravegaWriter<PravegaRecord> sink = createFlinkPravegaWriterForPravegaRecord(
                outputStreamConfig, true, PravegaWriterMode.EXACTLY_ONCE);

        toOutput
                .addSink(sink)
                .uid("pravega-writer")
                .name("Pravega writer to " + outputStreamConfig.getStream().getScopedName());

        JobClient jobClient = testEnvironment.executeAsync("TestKafkaStreamMirroringJob");
        System.out.println("\n\n\nJob ID: " + jobClient.getJobID().toString() + "\n\n");

        URI remoteControllerURI = URI.create(remoteTestResource.getControllerUri());

        final String readerGroup = "remoteReaderGroup";
        final String readerId = "remoteReader";
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(remoteTestResource.getStreamScope(), remoteTestResource.getStreamName()))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(remoteTestResource.getStreamScope(), remoteControllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }

        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(remoteTestResource.getStreamScope(),
                ClientConfig.builder().controllerURI(remoteControllerURI).build());
             EventStreamReader<PravegaRecord> reader = clientFactory.createReader(readerId,
                     readerGroup,
                     new JavaSerializer<>(),
                     ReaderConfig.builder().build())) {
            log.info("Reading all the events from {}/{}%n", remoteTestResource.getStreamScope(), remoteTestResource.getStreamName());
            EventRead<PravegaRecord> event = null;
            do {
                try {
                    event = reader.readNextEvent(READER_TIMEOUT_MS);
                    if (event.getEvent() != null) {
                        log.info("Read event '{}'%n", event);
                    }
                } catch (ReinitializationRequiredException e) {
                    //There are certain circumstances where the reader needs to be reinitialized
                    e.printStackTrace();
                }
            } while (Objects.requireNonNull(event).getEvent() != null);
            log.info("No more events from {}/{}%n", remoteTestResource.getStreamScope(), remoteTestResource.getStreamName());
        }

        remoteTestResource.stop();

//        reader.close();
    }
}
