/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.dataimporter.actions;

import io.pravega.client.admin.StreamManager;
import io.pravega.dataimporter.AppConfiguration;
import io.pravega.dataimporter.PravegaEmulatorResource;
import lombok.Cleanup;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit test class for KafkaMirroringAction
 */
public class KafkaMirroringActionTest {

    @ClassRule
    public static final PravegaEmulatorResource EMULATOR = PravegaEmulatorResource.builder().build();

    final String streamScope = "testScope";
    final String streamName = "testStream";

    /**
     * Tests KafkaMirroringAction metadata changes.
     * Checks if action creates output stream with correct stream tag.
     */
    @Test
    public void testKafkaMirroringAction() throws IOException {

        String controllerURI = EMULATOR.getControllerURI();

        final String scopedStreamName = streamScope + "/" + streamName;

        @Cleanup
        StreamManager streamManager = StreamManager.create(URI.create(controllerURI));
        streamManager.createScope(streamScope);
        final boolean beforeCheck = streamManager.checkStreamExists(streamScope, streamName);

        HashMap<String, String> argsMap = new HashMap<>();
        argsMap.put(AppConfiguration.ACTION_PARAMETER, KafkaMirroringAction.NAME);
        argsMap.put("output-controller", controllerURI);
        argsMap.put("output-stream", scopedStreamName);
        AppConfiguration configuration;

        configuration = AppConfiguration.createAppConfiguration(argsMap);

        String actionType = configuration.getParams().get(AppConfiguration.ACTION_PARAMETER);
        KafkaMirroringAction action = (KafkaMirroringAction) ActionFactory.instantiateAction(actionType, configuration, false);
        action.commitMetadataChanges();

        final boolean afterCheck = streamManager.checkStreamExists(streamScope, streamName);

        assertFalse(beforeCheck, "scope and stream already existed");
        assertTrue(afterCheck, "scope and stream weren't created");
        assertTrue(streamManager.getStreamTags(streamScope, streamName).contains(KafkaMirroringAction.NAME));
    }
}
