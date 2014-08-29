/* Copyright (c) 2011 Danish Maritime Authority.
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
package dk.dma.ais.store.job;


import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import dk.dma.ais.store.AisStoreQueryResult;
import dk.dma.commons.util.JSONObject;

/**
 * Keeps track of all current download jobs.
 * 
 * @author Kasper Nielsen
 */
public class JobManager {

    private final ConcurrentHashMap<String, Job> jobs = new ConcurrentHashMap<>();

    /**
     * Adds the specified job to the manager.
     * 
     * @param key
     *            the string key of the job
     * @param queryResult
     * @param releaseCounter
     */
    public void addJob(String key, AisStoreQueryResult queryResult, AtomicLong releaseCounter) {
        jobs.put(requireNonNull(key), new Job(key, queryResult, releaseCounter));
    }

    public void cleanup() {

    }

    public Job getResult(String key) {
        return jobs.get(key);
    }
    
    public Map<String, Job> getJobs() {
        return jobs;
    }
    
    public JSONObject toJSON() {
        return JSONObject.singleList("jobs", jobs.values().stream().map(new Function<Job, JSONObject>() {
            @Override
            public JSONObject apply(Job t) {
                return t.toJSON();
            }
        }).toArray()); 
    }

    public static class Job {

        /** The id of the job. */
        final String id;

        final AisStoreQueryResult queryResult;

        final AtomicLong releaseCounter;

        Job(String id, AisStoreQueryResult queryResult, AtomicLong releaseCounter) {
            this.id = requireNonNull(id);
            this.queryResult = requireNonNull(queryResult);
            this.releaseCounter = requireNonNull(releaseCounter);
        }

        /**
         * @return the queryResult
         */
        public AisStoreQueryResult getQuery() {
            return queryResult;
        }

        public JSONObject toJSON() {
            JSONObject j = new JSONObject();
            j.addElement("jobId", id);
            j.addElement("packetsReturned", releaseCounter.get());
            j.addElement("status", queryResult.getState());
            return j;
        }
    }
}
