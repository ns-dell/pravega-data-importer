package io.pravega.dataimporter;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.dataimporter.utils.PravegaRecord;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static net.mguenther.kafka.junit.SendValues.to;

public class KafkaMirroringJobTest {

    final private static Logger log = LoggerFactory.getLogger(KafkaMirroringJobTest.class);

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
    public void testKafkaToPravegaStreamJob() throws InterruptedException {

        HashMap<String, byte[]> headers = new HashMap<>();
        headers.put("h1", "v1".getBytes());
        kafka.send(to("test-input", new PravegaRecord("key1".getBytes(),"value1".getBytes(),headers,1, "test-topic",123456)));

        PravegaTestResource remoteTestResource = new PravegaTestResource(9090, 12345, "remoteScope", "remoteStream");
        remoteTestResource.start();

        URI remoteControllerURI = URI.create(remoteTestResource.getControllerUri());
        ClientConfig remoteClientConfig = ClientConfig.builder()
                .controllerURI(remoteControllerURI).build();

        ReaderGroupManager readerGroupManager = ReaderGroupManager
                .withScope(remoteTestResource.getStreamScope(), remoteClientConfig);
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(remoteTestResource.getStreamScope() + "/" + remoteTestResource.getStreamName()).build();
        readerGroupManager.createReaderGroup("remoteReader", readerGroupConfig);

        EventStreamClientFactory remoteFactory = EventStreamClientFactory
                .withScope(remoteTestResource.getStreamScope(), remoteClientConfig);

        EventStreamReader<PravegaRecord> reader = remoteFactory
                .createReader("remoteReaderId", "remoteReader",
                        new JavaSerializer<>(), ReaderConfig.builder().build());

        PravegaRecord recordEvent;
        int counter = 0;
        while ((recordEvent = reader.readNextEvent(1000).getEvent()) != null) {
            log.info(recordEvent.toString());
            counter++;
        }
        Assertions.assertEquals(3, counter);

        reader.close();
    }
}
