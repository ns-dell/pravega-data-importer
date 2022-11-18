package io.pravega.dataimporter.actions;

import io.pravega.dataimporter.AppConfiguration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ActionFactoryTest {

    @Test
    public void testActionFactory_KafkaMirroringAction(){
        HashMap<String, String> argsMap = new HashMap<>();
        argsMap.put(AppConfiguration.ACTION_PARAMETER, KafkaMirroringAction.NAME);
        AppConfiguration configuration;
        try {
            configuration = AppConfiguration.createAppConfiguration(argsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String actionType = configuration.getParams().get(AppConfiguration.ACTION_PARAMETER);
        AbstractAction dataImportAction = ActionFactory.instantiateAction(actionType, configuration);
        assertThat(dataImportAction, instanceOf(KafkaMirroringAction.class));
    }

    @Test
    public void testActionFactory_PravegaMirroringAction(){
        HashMap<String, String> argsMap = new HashMap<>();
        argsMap.put(AppConfiguration.ACTION_PARAMETER, PravegaMirroringAction.NAME);
        AppConfiguration configuration;
        try {
            configuration = AppConfiguration.createAppConfiguration(argsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String actionType = configuration.getParams().get(AppConfiguration.ACTION_PARAMETER);
        AbstractAction dataImportAction = ActionFactory.instantiateAction(actionType, configuration);
        assertThat(dataImportAction, instanceOf(PravegaMirroringAction.class));
    }

    @Test
    public void testActionFactory_fail(){
        HashMap<String, String> argsMap = new HashMap<>();
        argsMap.put(AppConfiguration.ACTION_PARAMETER, "test");
        AppConfiguration configuration;
        try {
            configuration = AppConfiguration.createAppConfiguration(argsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String actionType = configuration.getParams().get(AppConfiguration.ACTION_PARAMETER);
        assertThrows(IllegalArgumentException.class, () -> ActionFactory.instantiateAction(actionType, configuration));
    }
}