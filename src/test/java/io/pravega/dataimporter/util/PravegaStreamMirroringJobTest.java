package io.pravega.dataimporter.util;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.dataimporter.utils.PravegaRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;

public class PravegaStreamMirroringJobTest {

    final private static Logger log = LoggerFactory.getLogger(PravegaStreamMirroringJobTest.class);

    @Test
    public void TestPravegaClient() {
        final String LOCAL_STREAM_SCOPE = "local";
        final String LOCAL_STREAM_NAME = "localStream";
        final String localControllerURIString = "tcp://localhost:9090";

        final String REMOTE_STREAM_SCOPE = "remote";
        final String REMOTE_STREAM_NAME = "remoteStream";
        final String remoteControllerURIString = "tcp://localhost:9990";

        final int numSegments = 1;
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(numSegments)) //temporary fixed scaling policy at 1 segment
                .build();
        URI localControllerURI = URI.create(localControllerURIString);
        try (StreamManager streamManager = StreamManager.create(localControllerURI)) {
            streamManager.createScope(LOCAL_STREAM_SCOPE);
            streamManager.createStream(LOCAL_STREAM_SCOPE, LOCAL_STREAM_NAME, streamConfig);
        }
        ClientConfig localClientConfig = ClientConfig.builder()
                .controllerURI(localControllerURI).build();
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        EventStreamClientFactory factory = EventStreamClientFactory
                .withScope(LOCAL_STREAM_SCOPE, localClientConfig);
        EventStreamWriter<PravegaRecord> localWriter = factory
                .createEventWriter(LOCAL_STREAM_NAME, new JavaSerializer<>(), writerConfig);
        HashMap<String, byte[]> headers = new HashMap<>();
        headers.put("h1", "v1".getBytes());
        localWriter.writeEvent(new PravegaRecord("key1".getBytes(),"value1".getBytes(),headers,1));
        headers.put("h2", "v2".getBytes());
        localWriter.writeEvent(new PravegaRecord("key2".getBytes(),"value2".getBytes(),headers,2));
        headers.put("h3", "v3".getBytes());
        localWriter.writeEvent(new PravegaRecord("key3".getBytes(),"value3".getBytes(),headers,3));
        localWriter.flush();

        URI remoteControllerURI = URI.create(remoteControllerURIString);
        ClientConfig remoteClientConfig = ClientConfig.builder()
                .controllerURI(remoteControllerURI).build();

        ReaderGroupManager readerGroupManager = ReaderGroupManager
                .withScope(REMOTE_STREAM_SCOPE, remoteClientConfig);
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(REMOTE_STREAM_SCOPE + "/" + REMOTE_STREAM_NAME).build();
        readerGroupManager.createReaderGroup("remoteReader", readerGroupConfig);

        EventStreamReader<PravegaRecord> reader = factory
                .createReader("remoteReaderId", "remoteReader",
                        new JavaSerializer<>(), ReaderConfig.builder().build());

        PravegaRecord recordEvent;
        int counter = 0;
        while ((recordEvent = reader.readNextEvent(1000).getEvent()) != null) {
            log.info(recordEvent.toString());
            counter++;
        }
        assertTrue(counter == 3);

        reader.close();
    }
}
