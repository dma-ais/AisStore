/*
 * Copyright (c) 2008 Kasper Nielsen.
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
package dk.dma.cassandra_test;

import static java.util.Objects.requireNonNull;

import java.util.Date;

/**
 * 
 * An AisStoreQuery should only be used by a single thread.
 * 
 * @author Kasper Nielsen
 */
public abstract class AisStoreQuery {

    long mmsi = -1;

    long start = -1;

    long stop = -1;

    protected boolean hasMMSI() {
        return mmsi != -1;
    }

    protected long getMMSI() {
        return -1;
    }

    public AisStoreQuery setMMSI(long mmsi) {
        if (mmsi <= 0) {
            throw new IllegalArgumentException("MMSI must be positive, was " + mmsi);
        }
        this.mmsi = mmsi;
        return this;
    }

    public AisStoreQuery setStart(Date date) {
        requireNonNull(date, "date is null");

        return this;
    }

    public AisStoreQuery orderByDate() {

        return this;
    }

    // public abstract List<SingleMessage> execute();

}
