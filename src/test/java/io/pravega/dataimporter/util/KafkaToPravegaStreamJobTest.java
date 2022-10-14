package io.pravega.dataimporter.util;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static net.mguenther.kafka.junit.SendValues.to;
import static org.junit.Assert.assertTrue;

public class KafkaToPravegaStreamJobTest {

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
        assertTrue(kafka.observe(on("test-topic", 3)).size() == 3);
    }
}
