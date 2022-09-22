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
package io.pravega.dataimporter;

import io.pravega.dataimporter.actions.Action;
import io.pravega.dataimporter.actions.ActionFactory;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        // When this method is executed, we should expect the following:
        // 1. Parameters specifying the job to be executed (e.g., mirroring, import)
        // 2. Parameters specifying the required metadata information for a given job (e.g., origin/target cluster endpoints)
        // 3. Parameters related to the Flink job itself to be executed (e.g., parallelism)
        AppConfiguration configuration = new AppConfiguration(args);

        // STEP 1: Instantiate the Action based on input parameter
        String actionType = configuration.getParams().get("action-type");
        Action dataImportAction = new ActionFactory().instantiateAction(actionType);

        // STEP 2: Run the metadata workflow for the action.
        dataImportAction.commitMetadataChanges();

        // STEP 3: Submit the associated job to Flink.
        dataImportAction.submitDataImportJob();
    }
}
