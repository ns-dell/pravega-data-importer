package io.pravega.dataimporter.actions;

import io.pravega.client.admin.StreamManager;
import io.pravega.dataimporter.AppConfiguration;
import io.pravega.local.InProcPravegaCluster;
import io.pravega.local.LocalPravegaEmulator;
import io.pravega.test.common.TestUtils;
import lombok.Cleanup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class PravegaMirroringActionTest {

    final String streamScope = "testScope";
    final String streamName = "testStream";

    boolean restEnabled = true;
    boolean authEnabled = false;
    boolean tlsEnabled = false;
    LocalPravegaEmulator localPravega;

    @BeforeEach
    public void setUp() throws Exception {
        LocalPravegaEmulator.LocalPravegaEmulatorBuilder emulatorBuilder = LocalPravegaEmulator.builder()
                .controllerPort(TestUtils.getAvailableListenPort())
                .segmentStorePort(TestUtils.getAvailableListenPort())
                .zkPort(TestUtils.getAvailableListenPort())
                .restServerPort(TestUtils.getAvailableListenPort())
                .enableRestServer(restEnabled)
                .enableAuth(authEnabled)
                .enableTls(tlsEnabled);

        localPravega = emulatorBuilder.build();
        localPravega.start();
    }

    /**
     * Tests PravegaMirroringAction metadata changes.
     * Checks if action creates output stream with correct stream tag.
     */
    @Test
    public void testPravegaMirroringAction() {
        InProcPravegaCluster inProcPravegaCluster = localPravega.getInProcPravegaCluster();
        String controllerURI = inProcPravegaCluster.getControllerURI();

        final String scopedStreamName = streamScope + "/" + streamName;

        @Cleanup
        StreamManager streamManager = StreamManager.create(URI.create(controllerURI));
        final boolean beforeCheck = streamManager.checkStreamExists(streamScope, streamName);

        HashMap<String, String> argsMap = new HashMap<>();
        argsMap.put(AppConfiguration.ACTION_PARAMETER, KafkaMirroringAction.NAME);
        argsMap.put("output-stream", scopedStreamName);
        AppConfiguration configuration;
        try {
            configuration = AppConfiguration.createAppConfiguration(argsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String actionType = configuration.getParams().get(AppConfiguration.ACTION_PARAMETER);
        ActionFactory.instantiateAction(actionType, configuration);

        final boolean afterCheck = streamManager.checkStreamExists(streamScope, streamName);

        assertTrue(!beforeCheck && afterCheck);
        assertTrue(streamManager.getStreamTags(streamScope, streamName).contains(PravegaMirroringAction.NAME));
    }
}
