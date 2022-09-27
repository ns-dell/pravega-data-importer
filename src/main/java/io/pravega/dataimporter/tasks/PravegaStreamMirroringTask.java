package io.pravega.dataimporter.tasks;

import io.pravega.client.stream.StreamCut;
import io.pravega.dataimporter.AppConfiguration;
import io.pravega.dataimporter.actions.Action;
import io.pravega.dataimporter.jobs.PravegaStreamMirroringJob;

import java.io.IOException;

public class PravegaStreamMirroringTask implements Task{

    private AppConfiguration configuration;
    private PravegaStreamMirroringJob pravegaStreamMirroringJob;
    private final AppConfiguration.StreamConfig inputStreamConfig;
    private StreamCut startStreamCut;
    private StreamCut endStreamCut;

    public PravegaStreamMirroringTask(String[] args) throws IOException {
        configuration = new AppConfiguration(args);
        pravegaStreamMirroringJob = new PravegaStreamMirroringJob(configuration);
        inputStreamConfig = configuration.getStreamConfig("input");
        final String jobName = configuration.getJobName(PravegaStreamMirroringJob.class.getName());
//        log.info("input stream: {}", inputStreamConfig);
//        createStream(inputStreamConfig);
    }

    @Override
    public void performPriorActions() {
        Action.createStream(inputStreamConfig);
        startStreamCut = Action.resolveStartStreamCut(inputStreamConfig);
        endStreamCut = Action.resolveEndStreamCut(inputStreamConfig);
    }

    @Override
    public void runJob() {

    }

    @Override
    public void performPostActions() {

    }
}
