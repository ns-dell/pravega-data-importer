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

import io.pravega.dataimporter.AppConfiguration;
import io.pravega.dataimporter.jobs.PravegaMirroringJob;
import lombok.extern.slf4j.Slf4j;

/**
 * Implements the workflow related to mirroring one Pravega Stream from one cluster to another.
 */
@Slf4j
public class PravegaMirroringAction extends AbstractAction {
    public final static String NAME = "stream-mirroring";

    private final AppConfiguration config;

    /**
     * Creates a new instance of the PravegaMirroringAction class.
     * @param config app configs
     * @param remoteCluster whether to execute on remote or local (JVM instantiated) Flink cluster. Setting this
     *                      parameter to false is useful for testing.
     */
    public PravegaMirroringAction(AppConfiguration config, boolean remoteCluster) {
        this.config = config;
        super.job = new PravegaMirroringJob(this.config, remoteCluster);
    }

    /**
     * Returns the application configuration passed in during action creation.
     *
     * @return Application configuration of the PravegaMirroringAction.
     */
    public AppConfiguration getConfig() {
        return config;
    }

    @Override
    public void commitMetadataChanges() {
        final AppConfiguration.StreamConfig outputStreamConfig = getConfig().getStreamConfig("output");
        log.info("output stream: {}", outputStreamConfig);
        AbstractAction.createStream(outputStreamConfig, NAME);
    }

    @Override
    public String getJobName() {
        return job.getClass().getName();
    }

}
