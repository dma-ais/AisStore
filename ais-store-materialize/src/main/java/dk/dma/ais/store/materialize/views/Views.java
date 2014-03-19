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
package dk.dma.ais.store.materialize.views;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import dk.dma.ais.store.materialize.HashViewBuilder;

/**
 *  Views Factory
 * 
 * @author Jens Tuxen
 *
 */
public class Views {
    
    public static List<HashViewBuilder> allForLevel(String timeFormat) {
        LinkedList<HashViewBuilder> jobs = new LinkedList<>();

        jobs.add(new CellSourceTime());
        jobs.add(new CellTimeCount());
        jobs.add(new MMSITimeCount());
        jobs.add(new SourceTimeCount());
        
        for (HashViewBuilder job: jobs) {
            job.level(TimeUnit.MINUTES);
        }
        
        return jobs;
        
    }

}
