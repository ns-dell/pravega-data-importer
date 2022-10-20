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
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.dataimporter.client.AppConfiguration;
import io.pravega.dataimporter.jobs.AbstractJob;

/**
 * Interface that all Actions should implement (mirroring, importing, etc.).
 */
public abstract class Action {

    protected AbstractJob job;

    /**
     * Most actions may require to perform some metadata changes before actually start doing the actual import job.
     * Changes may involve creating Streams, add tags or any other metadata change that is required to correctly run
     * the data import job afterwards.
     */
    public abstract void commitMetadataChanges();

    public abstract String getJobName();

    public void submitDataImportJob() {
        // TODO: Logic to submit a job to run in Flink programmatically
        job.run();
    }

    /**
     * If the Pravega stream does not exist, creates a new stream with the specified stream configuration.
     * If the stream exists, it is unchanged.
     */
    public static void createStream(AppConfiguration.StreamConfig streamConfig, String streamTag) {
        try (StreamManager streamManager = StreamManager.create(streamConfig.getPravegaConfig().getClientConfig())) {
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                    .scalingPolicy(streamConfig.getScalingPolicy())
                    .tag(streamTag)
                    .build();
            streamManager.createStream(
                    streamConfig.getStream().getScope(),
                    streamConfig.getStream().getStreamName(),
                    streamConfiguration);
        }
    }

}
