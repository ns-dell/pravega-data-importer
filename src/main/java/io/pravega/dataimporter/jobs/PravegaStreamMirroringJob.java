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
package io.pravega.dataimporter.jobs;

import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.dataimporter.client.AppConfiguration;
import io.pravega.dataimporter.utils.ByteArrayDeserializationFormat;
import io.pravega.dataimporter.utils.ByteArraySerializationFormat;
import io.pravega.dataimporter.utils.PravegaRecord;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Continuously copy a Pravega stream to another Pravega stream.
 * This supports events with any serialization.
 * When writing events, a fixed routing key is used.
 */
public class PravegaStreamMirroringJob extends AbstractJob {

    final private static Logger log = LoggerFactory.getLogger(PravegaStreamMirroringJob.class);

    final StreamExecutionEnvironment env;

    final String jobName = getConfig().getJobName(PravegaStreamMirroringJob.class.getName());

    public PravegaStreamMirroringJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
        env = initializeFlinkStreaming();
    }

    public void run() {
        try {
            final AppConfiguration.StreamConfig inputStreamConfig = getConfig().getStreamConfig("input");

            final StreamCut startStreamCut = resolveStartStreamCut(inputStreamConfig);
            final StreamCut endStreamCut = resolveEndStreamCut(inputStreamConfig);
            final AppConfiguration.StreamConfig outputStreamConfig = getConfig().getStreamConfig("output");

            final boolean isStreamOrdered = getConfig().getParams().getBoolean("isStreamOrdered", true);
            log.info("isStreamOrdered: {}", isStreamOrdered);

            final FlinkPravegaReader<byte[]> flinkPravegaReader = FlinkPravegaReader.<byte[]>builder()
                    .withPravegaConfig(inputStreamConfig.getPravegaConfig())
                    .forStream(inputStreamConfig.getStream(), startStreamCut, endStreamCut)
                    .withDeserializationSchema(new ByteArrayDeserializationFormat())
                    .build();
            final DataStream<byte[]> events = env
                    .addSource(flinkPravegaReader)
                    .uid("pravega-reader")
                    .name("Pravega reader from " + inputStreamConfig.getStream().getScopedName());

            final FlinkPravegaWriter<byte[]> sink;
            FlinkPravegaWriter.Builder<byte[]> flinkPravegaWriterBuilder = FlinkPravegaWriter.<byte[]>builder()
                    .withPravegaConfig(outputStreamConfig.getPravegaConfig())
                    .forStream(outputStreamConfig.getStream())
                    .withSerializationSchema(new ByteArraySerializationFormat());
            if (isStreamOrdered) {
                //ordered write, multi-partition. routing key taken from current thread name
                flinkPravegaWriterBuilder.withEventRouter(event -> Thread.currentThread().getName());
            }
            flinkPravegaWriterBuilder.withWriterMode(PravegaWriterMode.EXACTLY_ONCE);

            sink = flinkPravegaWriterBuilder.build();
            events.addSink(sink)
                    .uid("pravega-writer")
                    .name("Pravega writer to " + outputStreamConfig.getStream().getScopedName());

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}