package io.pravega.dataimporter.util;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.dataimporter.client.Main;
import io.pravega.dataimporter.utils.PravegaRecord;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;

public class PravegaStreamMirroringJobTest {

    final private static Logger log = LoggerFactory.getLogger(PravegaStreamMirroringJobTest.class);

    private static final int READER_TIMEOUT_MS = 2000;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Test
    public void TestPravegaStreamMirroringJob() {

        //TODO: add code for flink job submission
        String argString = "--action-type stream-mirroring" +
                " --input-controller tcp://localhost:9090" +
                " --input-stream localScope/localStream" +
                " --input-startAtTail false" +
                " --output-stream remoteScope/remoteStream" +
                " --output-controller tcp://127.0.0.1:9990";
        String[] args = argString.split("\\s+");

        PravegaTestResource localTestResource = new PravegaTestResource(9090, 12345, "localScope", "localStream");
        localTestResource.start();

        PravegaTestResource remoteTestResource = new PravegaTestResource(9990, 23456, "remoteScope", "remoteStream");
        remoteTestResource.start();

        Main.main(args);

        URI localControllerURI = URI.create(localTestResource.getControllerUri());

        ClientConfig localClientConfig = ClientConfig.builder()
                .controllerURI(localControllerURI).build();
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        EventStreamClientFactory localFactory = EventStreamClientFactory
                .withScope(localTestResource.getStreamScope(), localClientConfig);
        EventStreamWriter<PravegaRecord> localWriter = localFactory
                .createEventWriter(localTestResource.getStreamName(), new JavaSerializer<>(), writerConfig);
        HashMap<String, byte[]> headers = new HashMap<>();
        headers.put("h1", "v1".getBytes());
        localWriter.writeEvent(new PravegaRecord("key1".getBytes(), "value1".getBytes(), headers, 1));
        headers.put("h2", "v2".getBytes());
        localWriter.writeEvent(new PravegaRecord("key2".getBytes(), "value2".getBytes(), headers, 2));
        headers.put("h3", "v3".getBytes());
        localWriter.writeEvent(new PravegaRecord("key3".getBytes(), "value3".getBytes(), headers, 3));
        localWriter.flush();


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
                        log.info("Read event '{}'%n", event.toString());
                    }
                } catch (ReinitializationRequiredException e) {
                    //There are certain circumstances where the reader needs to be reinitialized
                    e.printStackTrace();
                }
            } while (event.getEvent() != null);
            log.info("No more events from {}/{}%n", remoteTestResource.getStreamScope(), remoteTestResource.getStreamName());
        }

        localTestResource.stop();
        remoteTestResource.stop();
    }
}
