/* Copyright (c) 2011 Danish Maritime Authority
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this library.  If not, see <http://www.gnu.org/licenses/>.
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
        })); 
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
