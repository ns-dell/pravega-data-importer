package io.pravega.dataimporter.cli;

import io.pravega.dataimporter.AppConfiguration;
import io.pravega.dataimporter.actions.AbstractAction;
import io.pravega.dataimporter.actions.ActionFactory;
import picocli.CommandLine;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "kafka-stream-mirroring",
        description = "Initiates Kafka Stream Mirroring Command",
        mixinStandardHelpOptions = true)
public class KafkaMirroringCommand implements Callable<Integer> {
    @CommandLine.Option(
            names = "input-topic",
            description = "Kafka topic that data-importer reads from.",
            required = true)
    String inputTopic;
    @CommandLine.Option(
            names = "bootstrap-servers",
            description = "Kafka Controller URL of input stream.",
            defaultValue = "tcp://localhost:9092")
    String bootstrapServers = "tcp://localhost:9092";

    @CommandLine.Option(
            names = "isStreamOrdered",
            description = "Choose whether input stream is ordered or not.")
    boolean isStreamOrdered = true;

    @CommandLine.Option(
            names = "output-stream",
            description = "Scoped Pravega stream name that data-importer writes to.",
            required = true)
    String outputStream;
    @CommandLine.Option(
            names = "output-controller",
            description = "Pravega Controller URL of output stream.",
            defaultValue = "tcp://localhost:9090")
    String outputController = "tcp://localhost:9090";

    @CommandLine.Option(
            names = "flink-host",
            description = "Flink Host Name (e.g. localhost)",
            defaultValue = "localhost")
    String flinkHost = "localhost";
    @CommandLine.Option(
            names = "flink-port",
            description = "Flink Port Number (e.g. 8081)",
            defaultValue = "8081")
    int flinkPort = 8081;

    @Override
    public Integer call() {
        // When this method is executed, we should expect the following:
        // 1. Parameters specifying the job to be executed (e.g., mirroring, import)
        // 2. Parameters specifying the required metadata information for a given job (e.g., origin/target cluster endpoints)
        // 3. Parameters related to the Flink job itself to be executed (e.g., parallelism)
        HashMap<String, String> argsMap = new HashMap<>();
        argsMap.put("action-type", "kafka-stream-mirroring");
        argsMap.put("input-topic", inputTopic);
        argsMap.put("bootstrap.servers", bootstrapServers);
        argsMap.put("isStreamOrdered", String.valueOf(isStreamOrdered));
        argsMap.put("output-stream", outputStream);
        argsMap.put("output-controller", outputController);
        argsMap.put("flinkHost", flinkHost);
        argsMap.put("flinkPort", String.valueOf(flinkPort));


        AppConfiguration configuration;
        try {
            configuration = new AppConfiguration(argsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // STEP 1: Instantiate the Action based on input parameter
        String actionType = configuration.getParams().get(AppConfiguration.ACTION_PARAMETER);
        AbstractAction dataImportAction = new ActionFactory().instantiateAction(actionType, configuration);

        // STEP 2: Run the metadata workflow for the action.
        dataImportAction.commitMetadataChanges();

        // STEP 3: Submit the associated job to Flink.
        dataImportAction.submitDataImportJob();
        return 0;
    }
}
