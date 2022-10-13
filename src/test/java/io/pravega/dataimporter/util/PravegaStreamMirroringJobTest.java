package io.pravega.dataimporter.util;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import org.junit.Test;

import java.net.URI;

public class PravegaStreamMirroringJobTest {

    @Test
    public void TestPravegaClient() {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();
        URI localControllerURI = URI.create("tcp://localhost:9090");
        try (StreamManager streamManager = StreamManager.create(localControllerURI)) {
            streamManager.createScope("demo");
            streamManager.createStream("demo", "PravegaToPravega", streamConfig);
        }
        ClientConfig localClientConfig = ClientConfig.builder()
                .controllerURI(localControllerURI).build();
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        EventStreamClientFactory factory = EventStreamClientFactory
                .withScope("demo", localClientConfig);
        EventStreamWriter<Integer> writer = factory
                .createEventWriter("PravegaToPravega", new JavaSerializer<Integer>(), writerConfig);
        writer.writeEvent(1);
        writer.writeEvent(2);
        writer.writeEvent(3);
        writer.flush();
    }
}
