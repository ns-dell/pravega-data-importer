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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.dataimporter.AppConfiguration;
import org.apache.flink.core.execution.JobClient;

import java.io.IOException;
import java.util.Map;

/**
 * Factory class that is used to instantiate concrete implementations of AbstractAction.
 */
public class ActionFactory {

    /**
     * Method that instantiates concrete implementations of {@link AbstractAction}.
     * @param actionType input String based on name of action
     * @param configuration application configuration
     * @param remoteCluster whether to execute on remote or local (JVM instantiated) Flink cluster. Setting this
     *                      parameter to false is useful for testing.
     *
     * @return Concrete implementation of {@link AbstractAction} based on input parameter.
     */
    @VisibleForTesting
    static AbstractAction instantiateAction(String actionType, AppConfiguration configuration, boolean remoteCluster) {
        switch (actionType) {
            case PravegaMirroringAction.NAME:
                return new PravegaMirroringAction(configuration, remoteCluster);
            case KafkaMirroringAction.NAME:
                return new KafkaMirroringAction(configuration, remoteCluster);
            default:
                throw new IllegalArgumentException("Unknown action type.");
        }
    }

    /**
     * Static Method that encompasses the life cycle of a data importer use case. Instantiates action based on input
     * parameters, runs metadata workflow for the action, and submits associated job to Flink cluster.
     *
     * @param argsMap map of command line arguments used to create an {@link AppConfiguration}
     * @param remoteCluster whether to execute on remote or local (JVM instantiated) Flink cluster.
     *                      Setting this parameter to false is useful for testing.
     *
     * @return {@link JobClient} returned after Flink job is submitted to Flink's execution environment
     */
    public static JobClient createActionSubmitJob(Map<String, String> argsMap, boolean remoteCluster) {
        AppConfiguration configuration;
        try {
            configuration = AppConfiguration.createAppConfiguration(argsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // STEP 1: Instantiate the Action based on input parameter
        String actionType = configuration.getParams().get(AppConfiguration.ACTION_PARAMETER);
        AbstractAction dataImportAction = ActionFactory.instantiateAction(actionType, configuration, remoteCluster);

        // STEP 2: Run the metadata workflow for the action.
        dataImportAction.commitMetadataChanges();

        // STEP 3: Submit the associated job to Flink.
        return dataImportAction.submitDataImportJob();
    }
}
