package io.pravega.dataimporter;

import picocli.CommandLine;

@CommandLine.Command(name = "pravega-data-importer", mixinStandardHelpOptions = true)
public class CLI implements Runnable{

    @CommandLine.ArgGroup()
    ActionType actionType;

    @CommandLine.ArgGroup(exclusive = false, heading = "Options for Flink configuration")
    FlinkOptions flinkOptions;

    @CommandLine.Option(
            names = "output-stream",
            description = "Scoped Pravega stream name that data-importer writes to.",
            required = true)
    String outputStream;
    @CommandLine.Option(
            names = "output-controller",
            description = "Pravega Controller URL of output stream.")
    String outputController = "tcp://localhost:9090";

    static class FlinkOptions {
        @CommandLine.Option(
                names = "flinkHost",
                description = "Scoped Pravega stream name that data-importer reads from.",
                required = true)
        String flinkHost = "localhost";
        @CommandLine.Option(
                names = "flinkPort",
                description = "Pravega Controller URL of input stream.",
                required = true)
        int flinkPort = 8081;
    }

    static class ActionType {
        @CommandLine.ArgGroup(
                heading = "Options for Pravega Stream Mirroring",
                exclusive = false)
        PravegaStreamMirroringOptions pravegaStreamMirroringOptions;
        @CommandLine.ArgGroup(
                heading = "Options for Kafka Stream Mirroring",
                exclusive = false)
        KafkaStreamMirroringOptions kafkaStreamMirroringOptions;
    }

    static class PravegaStreamMirroringOptions {
        @CommandLine.Option(
                names = "input-stream",
                description = "Scoped Pravega stream name that data-importer reads from.",
                required = true)
        String inputStream;
        @CommandLine.Option(
                names = "input-controller",
                description = "Pravega Controller URL of input stream.")
        String inputController = "tcp://localhost:9090";
        @CommandLine.Option(
                names = "input-startAtTail",
                description = "Choose whether the input stream starts at the tail of the stream or not.")
        boolean inputStartAtTail = false;
    }

    static class KafkaStreamMirroringOptions {
        @CommandLine.Option(
                names = "input-topic",
                description = "Kafka topic that data-importer reads from.",
                required = true)
        String inputTopic;
        @CommandLine.Option(
                names = "bootstrap.servers",
                description = "Kafka Controller URL of input stream.")
        String bootstrapServers = "tcp://localhost:9092";
    }

    @Override
    public void run() {

    }
}
