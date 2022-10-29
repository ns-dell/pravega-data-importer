package io.pravega.dataimporter.actions;

import io.pravega.dataimporter.AppConfiguration;
import io.pravega.dataimporter.jobs.KafkaToPravegaStreamJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMirroringAction extends Action{
    final private static Logger log = LoggerFactory.getLogger(KafkaMirroringAction.class);

    public final static String NAME = "kafka-stream-mirroring";

    private final AppConfiguration config;

    public KafkaMirroringAction(AppConfiguration config) {
        this.config = config;
        super.job = new KafkaToPravegaStreamJob(this.config);
    }

    public AppConfiguration getConfig() {
        return config;
    }

    @Override
    public void commitMetadataChanges() {
        final AppConfiguration.StreamConfig outputStreamConfig = getConfig().getStreamConfig("output");
        log.info("output stream: {}", outputStreamConfig);
        Action.createStream(outputStreamConfig, "kafka-mirror");
    }

    @Override
    public String getJobName() {
        return job.getClass().getName();
    }
    
}
