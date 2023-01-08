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
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.dataimporter.PravegaEmulatorResource;
import io.pravega.dataimporter.actions.ActionFactory;
import io.pravega.dataimporter.actions.KafkaMirroringAction;
import io.pravega.dataimporter.utils.PravegaRecord;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.SendKeyValues;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.Before;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static net.mguenther.kafka.junit.ObserveKeyValues.on;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class KafkaMirroringJobIntegrationTest {

    @ClassRule
    public static final PravegaEmulatorResource EMULATOR = PravegaEmulatorResource.builder().build();

    @ClassRule
    final public static MiniClusterWithClientResource FLINK_CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    private static final int READER_TIMEOUT_MS = 10000;

    final String kafkaTopicName = "test-topic";
    final String streamScope = "testScope";
    final String streamName = "testStream";
    private EmbeddedKafkaCluster kafka;

    @Before
    public void setupKafka() {
        this.kafka = provisionWith(defaultClusterConfig());
        this.kafka.start();
    }

    @After
    public void tearDownKafka() {
        this.kafka.stop();
    }

    @Test
    public void testKafkaToPravegaStreamJob() throws Exception {

        final String controllerURI = EMULATOR.getControllerURI();

        List<KeyValue<String, String>> records = new ArrayList<>();

        records.add(new KeyValue<>("a", "b"));
        records.add(new KeyValue<>("c", "d"));
        records.add(new KeyValue<>("e", "f"));

        kafka.send(SendKeyValues.to(kafkaTopicName, records));
        assertEquals(records.size(), kafka.observe(on("test-topic", records.size())).size());
        String kafkaBrokerList = kafka.getBrokerList();

        HashMap<String, String> argsMap = new HashMap<>();
        argsMap.put("action-type", KafkaMirroringAction.NAME);
        argsMap.put("input-topic", kafkaTopicName);
        argsMap.put("output-stream", Stream.of(streamScope, streamName).getScopedName());
        argsMap.put("output-controller", controllerURI);
        argsMap.put("bootstrap.servers", kafkaBrokerList);
        argsMap.put("isStreamOrdered", String.valueOf(true));
        argsMap.put("checkpointIntervalMs", "1000");
        argsMap.put("checkpointTimeoutMs", "5000");

        URI remoteControllerURI = URI.create(EMULATOR.getControllerURI());

        @Cleanup
        StreamManager streamManager = StreamManager.create(remoteControllerURI);
        streamManager.createScope(streamScope);

        JobClient jobClient = ActionFactory.createActionSubmitJob(argsMap, false);

        assertTrue(streamManager.checkStreamExists(streamScope, streamName));
        AssertExtensions.assertEventuallyEquals(true, () -> jobClient.getJobStatus().join().equals(JobStatus.RUNNING), READER_TIMEOUT_MS);

        final String readerGroup = "outputReaderGroup";
        final String readerId = "outputReader";
        final ReaderGroupConfig readerGroupConfig = ReaderGroupConfig.builder()
                .stream(Stream.of(streamScope, streamName))
                .build();
        try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(streamScope, remoteControllerURI)) {
            readerGroupManager.createReaderGroup(readerGroup, readerGroupConfig);
        }

        try (EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(streamScope,
                ClientConfig.builder().controllerURI(remoteControllerURI).build());
             EventStreamReader<PravegaRecord> reader = clientFactory.createReader(readerId,
                     readerGroup,
                     new JavaSerializer<>(),
                     ReaderConfig.builder().build())) {
            log.info("Reading all the events from {}/{}", streamScope, streamName);
            EventRead<PravegaRecord> event = null;
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
            log.info("No more events from {}/{}", streamScope, streamName);

            assertEquals(records.size(), count);
        }
    }
}
