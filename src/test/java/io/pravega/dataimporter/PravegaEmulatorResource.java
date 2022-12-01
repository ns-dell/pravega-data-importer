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

import java.time.Duration;

import org.junit.rules.ExternalResource;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * Runs a standalone Pravega cluster in-process.
 * <p>
 * <code>pravega.controller.uri</code> system property will contain the
 * Pravega Controller URI.
 */
public class PravegaEmulatorResource extends ExternalResource {

    private static final String PRAVEGA_VERSION = "0.12.0";
    private static final int CONTROLLER_HOST_PORT = 9091;
    private static final int CONTROLLER_CONTAINER_PORT = 9090;
    private static final int SEGMENT_STORE_PORT = 12345;
    private static final String PRAVEGA_IMAGE = "pravega/pravega:" + PRAVEGA_VERSION;

    @SuppressWarnings("deprecation")
    private final GenericContainer<?> container = new FixedHostPortGenericContainer<>(PRAVEGA_IMAGE)
            .withFixedExposedPort(CONTROLLER_HOST_PORT, CONTROLLER_CONTAINER_PORT)
            .withFixedExposedPort(SEGMENT_STORE_PORT, SEGMENT_STORE_PORT)
            .withStartupTimeout(Duration.ofSeconds(90))
            .waitingFor(Wait.forLogMessage(".*Pravega Sandbox is running locally now.*", 1))
            .withCommand("standalone");

    @Override
    public void before() {
        container.start();
    }

    @Override
    public void after() {
        try {
            if (container != null) {
                container.stop();
            }
        } catch (Exception e) {
            // ignored
        }
    }

    public String getControllerURI() {
        return "tcp://" + container.getHost() + ":" + container.getMappedPort(CONTROLLER_CONTAINER_PORT);
    }

}