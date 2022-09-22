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

/**
 * Implements the workflow related to mirroring one Pravega Stream from one cluster to another.
 */
public class MirroringAction extends Action {

    public final static String NAME = "stream-mirroring";

    @Override
    public void commitMetadataChanges() {

    }

    @Override
    public String getJobName() {
        return "PravegaStreamMirroringJob";
    }
}
