package io.pravega.dataimporter.util;

import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

/**
 * Runs a standalone Pravega cluster in-process.
 * <p>
 * <code>pravega.controller.uri</code> system property will contain the
 * Pravega Controller URI.
 */
public class PravegaTestResource {

    private static final String PRAVEGA_VERSION = "0.12.0";

    private final int controllerPort;
    private final int segmentStorePort;
    private static final String PRAVEGA_IMAGE = "pravega/pravega:" + PRAVEGA_VERSION;
    private String streamScope;
    private String streamName;
    private final GenericContainer<?> container;

    @SuppressWarnings("deprecation")
    public PravegaTestResource(int controllerPort, int segmentStorePort){
        this.controllerPort = controllerPort;
        this.segmentStorePort = segmentStorePort;

        container = new FixedHostPortGenericContainer<>(PRAVEGA_IMAGE)
                .withFixedExposedPort(this.controllerPort, this.controllerPort)
                .withFixedExposedPort(this.segmentStorePort, this.segmentStorePort)
                .withStartupTimeout(Duration.ofSeconds(90))
                .waitingFor(Wait.forLogMessage(".*Starting gRPC server listening on port: 9090.*", 1))
                .withCommand("standalone");
    }

    public Map<String, String> start() {
        container.start();

        try (final StreamManager streamManager = StreamManager.create(URI.create(getControllerUri()))) {
            streamManager.createScope(streamScope);
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build();
            streamManager.createStream(streamScope, streamName, streamConfig);
        }

        return Collections.singletonMap("pravega.controller.uri", getControllerUri());
    }

    public void stop() {
        try {
            if (container != null) {
                container.stop();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }

    public String getControllerUri() {
        return "tcp://" + container.getHost() + ":" + container.getMappedPort(controllerPort);
    }

    public int getControllerPort() {
        return controllerPort;
    }

    public int getSegmentStorePort() {
        return segmentStorePort;
    }

    public String getStreamScope() {
        return streamScope;
    }

    public String getStreamName() {
        return streamName;
    }

}
