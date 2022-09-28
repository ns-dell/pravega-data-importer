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
import io.pravega.dataimporter.jobs.PravegaStreamMirroringJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the workflow related to mirroring one Pravega Stream from one cluster to another.
 */
public class MirroringAction extends Action {

    final private static Logger log = LoggerFactory.getLogger(MirroringAction.class);

    public final static String NAME = "stream-mirroring";

    private final AppConfiguration config;

    private final PravegaStreamMirroringJob job;

    public MirroringAction(AppConfiguration config) {
        this.config = config;
        job = new PravegaStreamMirroringJob(this.config);
    }

    public AppConfiguration getConfig() {
        return config;
    }

    @Override
    public void commitMetadataChanges() {
        final AppConfiguration.StreamConfig inputStreamConfig = getConfig().getStreamConfig("input");
        log.info("input stream: {}", inputStreamConfig);
        Action.createStream(inputStreamConfig, "mirror");
        final AppConfiguration.StreamConfig outputStreamConfig = getConfig().getStreamConfig("output");
        log.info("output stream: {}", outputStreamConfig);
        Action.createStream(outputStreamConfig, "mirror");
    }

    @Override
    public String getJobName() {
        return "PravegaStreamMirroringJob";
    }

    @Override
    public void submitDataImportJob() {
        job.run();
    }
}
