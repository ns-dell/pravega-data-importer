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
 * Interface that all Actions should implement (mirroring, importing, etc.).
 */
public abstract class Action {

    /**
     * Most actions may require to perform some metadata changes before actually start doing the actual import job.
     * Changes may involve creating Streams, add tags or any other metadata change that is required to correctly run
     * the data import job afterwards.
     */
    public abstract void commitMetadataChanges();

    public abstract String getJobName();

    public void submitDataImportJob() {
        // TODO: Logic to submit a job to run in FLink programmatically
    }

}
