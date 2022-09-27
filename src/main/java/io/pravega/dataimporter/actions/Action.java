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

import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.StreamCut;
import io.pravega.dataimporter.AppConfiguration;

/**
 * Interface that all Actions should implement (mirroring, importing, etc.).
 */
public interface Action {

    /**
     * If the Pravega stream does not exist, creates a new stream with the specified stream configuration.
     * If the stream exists, it is unchanged.
     */
    static void createStream(AppConfiguration.StreamConfig streamConfig) {
        try (StreamManager streamManager = StreamManager.create(streamConfig.getPravegaConfig().getClientConfig())) {
            StreamConfiguration streamConfiguration = StreamConfiguration.builder()
                    .scalingPolicy(streamConfig.getScalingPolicy())
                    .build();
            streamManager.createStream(
                    streamConfig.getStream().getScope(),
                    streamConfig.getStream().getStreamName(),
                    streamConfiguration);
        }
    }

    /**
     * Most actions may require to perform some metadata changes before actually start doing the actual import job.
     * Changes may involve creating Streams, add tags or any other metadata change that is required to correctly run
     * the data import job afterwards.
     */
    void commitMetadataChanges();

    void runAction(AppConfiguration.StreamConfig streamConfig);

    /**
     * Get head and tail stream cuts for a Pravega stream.
     */
    static StreamInfo getStreamInfo(AppConfiguration.StreamConfig streamConfig) {
        try (StreamManager streamManager = StreamManager.create(streamConfig.getPravegaConfig().getClientConfig())) {
            return streamManager.getStreamInfo(streamConfig.getStream().getScope(), streamConfig.getStream().getStreamName());
        }
    }

    /**
     * Convert UNBOUNDED start StreamCut to a concrete StreamCut, pointing to the current head or tail of the stream
     * (depending on isStartAtTail).
     */
    static StreamCut resolveStartStreamCut(AppConfiguration.StreamConfig streamConfig) {
        if (streamConfig.isStartAtTail()) {
            return getStreamInfo(streamConfig).getTailStreamCut();
        } else if (streamConfig.getStartStreamCut() == StreamCut.UNBOUNDED) {
            return getStreamInfo(streamConfig).getHeadStreamCut();
        } else {
            return streamConfig.getStartStreamCut();
        }
    }

    /**
     * For bounded reads (indicated by isEndAtTail), convert UNBOUNDED end StreamCut to a concrete StreamCut,
     * pointing to the current tail of the stream.
     * For unbounded reads, returns UNBOUNDED.
     */
    static StreamCut resolveEndStreamCut(AppConfiguration.StreamConfig streamConfig) {
        if (streamConfig.isEndAtTail()) {
            return getStreamInfo(streamConfig).getTailStreamCut();
        } else {
            return streamConfig.getEndStreamCut();
        }
    }

//    public abstract String getJobName();

//    public void submitDataImportJob() {
//        // TODO: Logic to submit a job to run in FLink programmatically
//    }

}
