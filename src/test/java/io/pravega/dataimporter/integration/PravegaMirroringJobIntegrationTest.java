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
package io.pravega.dataimporter.integration;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.dataimporter.AppConfiguration;
import io.pravega.dataimporter.PravegaEmulatorResource;
import io.pravega.dataimporter.actions.AbstractAction;
import io.pravega.dataimporter.actions.ActionFactory;
import io.pravega.dataimporter.actions.PravegaMirroringAction;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class PravegaMirroringJobIntegrationTest {

    @ClassRule
    final public static MiniClusterWithClientResource FLINK_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @ClassRule
    public static final PravegaEmulatorResource INPUT_EMULATOR = PravegaEmulatorResource.builder().build();

    @ClassRule
    public static final PravegaEmulatorResource OUTPUT_EMULATOR = PravegaEmulatorResource.builder().build();
    private static final int READER_TIMEOUT_MS = 10000;
    final String inputStreamScope = "inputScope";
    final String inputStreamName = "inputStream";
    final String outputStreamScope = "outputScope";
    final String outputStreamName = "outputStream";

    @Test
    public void testPravegaStreamMirroringJob() throws Exception {

        final String inputControllerURI = INPUT_EMULATOR.getControllerURI();
        final String outputControllerURI = OUTPUT_EMULATOR.getControllerURI();

        HashMap<String, String> argsMap = new HashMap<>();
        argsMap.put("action-type", PravegaMirroringAction.NAME);
        argsMap.put("input-stream", Stream.of(inputStreamScope, inputStreamName).getScopedName());
        argsMap.put("input-controller", inputControllerURI);
        argsMap.put("output-stream", Stream.of(outputStreamScope, outputStreamName).getScopedName());
        argsMap.put("output-controller", outputControllerURI);
        argsMap.put("isStreamOrdered", String.valueOf(true));
        argsMap.put("checkpointIntervalMs", "1000");
        argsMap.put("checkpointTimeoutMs", "5000");

        AppConfiguration appConfiguration = AppConfiguration.createAppConfiguration(argsMap);

        @Cleanup
        StreamManager inputStreamManager = StreamManager.create(URI.create(inputControllerURI));
        inputStreamManager.createScope(inputStreamScope);
        AbstractAction.createStream(appConfiguration.getStreamConfig("input"), PravegaMirroringAction.NAME);

        assertTrue(inputStreamManager.checkStreamExists(inputStreamScope, inputStreamName));

        @Cleanup
        StreamManager outputStreamManager = StreamManager.create(URI.create(outputControllerURI));
        outputStreamManager.createScope(outputStreamScope);

        JobClient jobClient = ActionFactory.createActionSubmitJob(argsMap, false);

        assertTrue(outputStreamManager.checkStreamExists(outputStreamScope, outputStreamName));
        AssertExtensions.assertEventuallyEquals(true, () -> jobClient.getJobStatus().join().equals(JobStatus.RUNNING), READER_TIMEOUT_MS);

        ClientConfig inputClientConfig = INPUT_EMULATOR.getClientConfig();
        EventWriterConfig writerConfig = EventWriterConfig.builder().build();
        EventStreamClientFactory localFactory = EventStreamClientFactory
                .withScope(inputStreamScope, inputClientConfig);

        ArrayList<byte[]> records = new ArrayList<>();
        records.add("record1".getBytes());
        records.add("record2".getBytes());
        records.add("record3".getBytes());
        EventStreamWriter<byte[]> inputWriter = localFactory
                .createEventWriter(inputStreamName, new JavaSerializer<>(), writerConfig);
        for (byte[] record : records) {
            inputWriter.writeEvent(record).join();
            log.info("Wrote event {}%n", record);
        }
        inputWriter.close();

        final String readerGroup = "outputReaderGroup";
        final String readerId = "outputReader";
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(outputStreamScope, outputStreamName))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(outputStreamScope, URI.create(outputControllerURI))) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }

        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(outputStreamScope,
                ClientConfig.builder().controllerURI(URI.create(outputControllerURI)).build());
             EventStreamReader<byte[]> reader = clientFactory.createReader(readerId,
                     readerGroup,
                     new JavaSerializer<>(),
                     ReaderConfig.builder().build())) {
            log.info("Reading all the events from {}/{}", outputStreamScope, outputStreamName);
            EventRead<byte[]> event;
            int count = 0;
            do {
                try {
                    event = reader.readNextEvent(READER_TIMEOUT_MS);
                    if (event.getEvent() != null) {
                        log.info("Read event '{}'", event);
                        count++;
                    }
                } catch (ReinitializationRequiredException e) {
                    //There are certain circumstances where the reader needs to be reinitialized
                    e.printStackTrace();
                }
            } while (count != records.size());
            log.info("No more events from {}/{}", outputStreamScope, outputStreamName);

            assertEquals(records.size(), count);
        }
    }
}
