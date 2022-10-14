package io.pravega.dataimporter.util;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.dataimporter.utils.PravegaRecord;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashMap;

public class PravegaStreamMirroringJobTest {

    final private static Logger log = LoggerFactory.getLogger(PravegaStreamMirroringJobTest.class);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    private static final String PRAVEGA_VERSION = "0.12.0";
    public static final int LOCAL_CONTROLLER_PORT = 9090;
    public static final int LOCAL_SEGMENT_STORE_PORT = 12345;
    public static final int REMOTE_CONTROLLER_PORT = 9990;
    public static final int REMOTE_SEGMENT_STORE_PORT = 23456;
    public static final String PRAVEGA_IMAGE = "pravega/pravega:" + PRAVEGA_VERSION;

    @Test
    public void TestPravegaStreamMirroringJob() {

        PravegaTestResource localTestResource = new PravegaTestResource(9090, 12345);
        localTestResource.start();

        URI localControllerURI = URI.create(localTestResource.getControllerUri());

        ClientConfig localClientConfig = ClientConfig.builder()
                .controllerURI(localControllerURI).build();
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        EventStreamClientFactory factory = EventStreamClientFactory
                .withScope(localTestResource.getStreamScope(), localClientConfig);
        EventStreamWriter<PravegaRecord> localWriter = factory
                .createEventWriter(localTestResource.getStreamName(), new JavaSerializer<>(), writerConfig);
        HashMap<String, byte[]> headers = new HashMap<>();
        headers.put("h1", "v1".getBytes());
        localWriter.writeEvent(new PravegaRecord("key1".getBytes(),"value1".getBytes(),headers,1));
        headers.put("h2", "v2".getBytes());
        localWriter.writeEvent(new PravegaRecord("key2".getBytes(),"value2".getBytes(),headers,2));
        headers.put("h3", "v3".getBytes());
        localWriter.writeEvent(new PravegaRecord("key3".getBytes(),"value3".getBytes(),headers,3));
        localWriter.flush();

        PravegaTestResource remoteTestResource = new PravegaTestResource(9990, 23456);
        remoteTestResource.start();

        URI remoteControllerURI = URI.create(remoteTestResource.getControllerUri());
        ClientConfig remoteClientConfig = ClientConfig.builder()
                .controllerURI(remoteControllerURI).build();

        ReaderGroupManager readerGroupManager = ReaderGroupManager
                .withScope(remoteTestResource.getStreamScope(), remoteClientConfig);
        ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(remoteTestResource.getStreamScope() + "/" + remoteTestResource.getStreamName()).build();
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
        Assertions.assertEquals(3, counter);

        reader.close();
    }
}
