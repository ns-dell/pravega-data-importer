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

import io.pravega.client.admin.StreamInfo;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.StreamCut;
import io.pravega.dataimporter.client.AppConfiguration;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract job class for Flink Pravega applications.
 */
public abstract class AbstractJob implements Runnable {
    final private static Logger log = LoggerFactory.getLogger(AbstractJob.class);

    private final AppConfiguration config;

    public AbstractJob(AppConfiguration config) {
        this.config = config;
    }

    public AppConfiguration getConfig() {
        return config;
    }

    /**
     * Get head and tail stream cuts for a Pravega stream.
     */
    public static StreamInfo getStreamInfo(AppConfiguration.StreamConfig streamConfig) {
        try (StreamManager streamManager = StreamManager.create(streamConfig.getPravegaConfig().getClientConfig())) {
            return streamManager.getStreamInfo(streamConfig.getStream().getScope(), streamConfig.getStream().getStreamName());
        }
    }

    /**
     * Convert UNBOUNDED start StreamCut to a concrete StreamCut, pointing to the current head or tail of the stream
     * (depending on isStartAtTail).
     */
    public static StreamCut resolveStartStreamCut(AppConfiguration.StreamConfig streamConfig) {
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
    public static StreamCut resolveEndStreamCut(AppConfiguration.StreamConfig streamConfig) {
        if (streamConfig.isEndAtTail()) {
            return getStreamInfo(streamConfig).getTailStreamCut();
        } else {
            return streamConfig.getEndStreamCut();
        }
    }

    public static StreamExecutionEnvironment initializeFlinkStreaming(AppConfiguration config) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String host = config.getParams().get("flinkHost", "localhost");
        int port = config.getParams().getInt("flinkPort", 8081);
        String jarFiles = "flinkJarTarget/pravega-data-importer-1.0-SNAPSHOT.jar";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(host,port,jarFiles);
        // Make parameters show in Flink UI.
        env.getConfig().setGlobalJobParameters(config.getParams());

        env.setParallelism(config.getParallelism());
        log.info("Parallelism={}, MaxParallelism={}", env.getParallelism(), env.getMaxParallelism());

        if (!config.isEnableOperatorChaining()) {
            env.disableOperatorChaining();
        }
        if (config.isEnableCheckpoint()) {
            env.enableCheckpointing(config.getCheckpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(config.getCheckpointIntervalMs() / 2);
            env.getCheckpointConfig().setCheckpointTimeout(config.getCheckpointTimeoutMs());
            // A checkpoint failure will cause the job to fail.
            env.getCheckpointConfig().setTolerableCheckpointFailureNumber(0);
            // If the job is cancelled manually by the user, do not delete the checkpoint.
            // This retained checkpoint can be used manually when restarting the job.
            // In SDP, a retained checkpoint can be used by creating a FlinkSavepoint object.
            env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        }

        // Configure environment for running in a local environment (e.g. in IntelliJ).
        if (env instanceof LocalStreamEnvironment) {
            // We can't use MemoryStateBackend because it can't store large state.
            if (env.getStateBackend() == null || env.getStateBackend() instanceof MemoryStateBackend) {
                log.warn("Using FsStateBackend instead of MemoryStateBackend");
                env.setStateBackend(new FsStateBackend("file:///tmp/flink-state", true));
            }
            // Stop immediately on any errors.
            log.warn("Using noRestart restart strategy");
            env.setRestartStrategy(RestartStrategies.noRestart());
            // Initialize Hadoop file system.
            FileSystem.initialize(config.getParams().getConfiguration());
        }
        return env;
    }

    public ExecutionEnvironment initializeFlinkBatch() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Make parameters show in Flink UI.
        env.getConfig().setGlobalJobParameters(getConfig().getParams());

        int parallelism = getConfig().getParallelism();
        if (parallelism > 0) {
            env.setParallelism(parallelism);
        }
        log.info("Parallelism={}", env.getParallelism());

        // Configure environment for running in a local environment (e.g. in IntelliJ).
        if (env instanceof LocalEnvironment) {
            // Initialize Hadoop file system.
            FileSystem.initialize(getConfig().getParams().getConfiguration());
        }
        return env;
    }
}