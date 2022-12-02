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
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ActionFactoryTest {

    @Test
    public void testKafkaMirroringAction() {
        HashMap<String, String> argsMap = new HashMap<>();
        argsMap.put(AppConfiguration.ACTION_PARAMETER, KafkaMirroringAction.NAME);
        AppConfiguration configuration;
        try {
            configuration = AppConfiguration.createAppConfiguration(argsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String actionType = configuration.getParams().get(AppConfiguration.ACTION_PARAMETER);
        AbstractAction dataImportAction = ActionFactory.instantiateAction(actionType, configuration, false);
        assertTrue(dataImportAction instanceof KafkaMirroringAction);
    }

    @Test
    public void testPravegaMirroringAction() {
        HashMap<String, String> argsMap = new HashMap<>();
        argsMap.put(AppConfiguration.ACTION_PARAMETER, PravegaMirroringAction.NAME);
        AppConfiguration configuration;
        try {
            configuration = AppConfiguration.createAppConfiguration(argsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String actionType = configuration.getParams().get(AppConfiguration.ACTION_PARAMETER);
        AbstractAction dataImportAction = ActionFactory.instantiateAction(actionType, configuration, false);
        assertTrue(dataImportAction instanceof PravegaMirroringAction);
    }

    @Test
    public void testActionFactoryFail() {
        HashMap<String, String> argsMap = new HashMap<>();
        argsMap.put(AppConfiguration.ACTION_PARAMETER, "test");
        AppConfiguration configuration;
        try {
            configuration = AppConfiguration.createAppConfiguration(argsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String actionType = configuration.getParams().get(AppConfiguration.ACTION_PARAMETER);
        assertThrows(IllegalArgumentException.class, () -> ActionFactory.instantiateAction(actionType, configuration, false));
    }
}
