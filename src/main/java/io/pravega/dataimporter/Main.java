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

import io.pravega.dataimporter.cli.KafkaMirroringCommand;
import io.pravega.dataimporter.cli.PravegaMirroringCommand;
import picocli.CommandLine.Command;
import picocli.CommandLine;

/**
 * Main method of data importer. CLI sub-commands are registered in command annotation.
 */
@Command(name = "pravega-data-importer",
        mixinStandardHelpOptions = true,
        subcommands = {PravegaMirroringCommand.class, KafkaMirroringCommand.class})
public class Main {
    public static void main(String[] args) {
        new CommandLine(new Main()).execute(args);
    }
}
