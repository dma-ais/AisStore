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
package dk.dma.ais.store.materialize.util;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
/**
 * 
 * @author Jens Tuxen
 *
 */
public class StatisticsWriter {

    private AtomicInteger count;
    private long startTime;
    private long endTime;
    private Object parent;
    private PrintWriter pw;

    private final String[] header = { "getParentClass", "getApplicationName",
            "getStartTime", "getEndTime", "getDuration", "getCountValue",
            "getPacketsPerSecond" };
    
    private final Map<String, String> extras = Collections.synchronizedMap(new TreeMap<String, String>());

    public long getStartTime() {
        return startTime;
    }

    public long getDuration() {
        return endTime - startTime;
    }

    public long getPacketsPerSecond() {
        try {
            return getCountValue() / (getDuration() / 1000);
        } catch (ArithmeticException e) {
            return -1L;
        }

    }

    public long getCountValue() {
        return count.get();
    }

    public StatisticsWriter(AtomicInteger count, Object parent, PrintWriter pw) {
        this.count = count;
        this.parent = parent;
        this.pw = pw;

        this.setStartTime(System.currentTimeMillis());

        pw.print(header);
        for (Entry<String, String> h: extras.entrySet()) {
            pw.print(h.getKey());
            pw.print(",");
        }
        pw.println();
        
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public String toCSV() {
        StringBuilder sb = new StringBuilder();

        for (String method : header) {
            try {
                sb.append(this.getClass().getMethod(method).invoke(this).toString());
            } catch (IllegalAccessException | IllegalArgumentException
                    | InvocationTargetException | NoSuchMethodException | NullPointerException
                    | SecurityException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            sb.append(",");
        }
        
        for (Entry<String, String> e : extras.entrySet()) {
            sb.append(e.getValue());
            sb.append(",");
        }
        
        sb.append("\n");

        return sb.toString();
    }

    public void print() {
        pw.print(toCSV());
        pw.flush();
    }

    public Class<? extends Object> getParentClass() {
        return parent.getClass();
    }

    public String getApplicationName() {
        return "StatsWriter";
    }
    
    public void put(String h,String v) {
        this.extras.put(h, v);
    }

}
