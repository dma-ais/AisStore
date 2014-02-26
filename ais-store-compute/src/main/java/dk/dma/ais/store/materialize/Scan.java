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
package dk.dma.ais.store.materialize;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Date;

import org.joda.time.field.DividedDateTimeField;

import com.beust.jcommander.Parameter;
import com.google.inject.Injector;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.materialize.cli.AbstractViewCommandLineTool;
import dk.dma.enav.util.function.Consumer;

/**
 * Iterate through a timeslice from start to end
 */
public abstract class Scan extends AbstractViewCommandLineTool implements Consumer<AisPacket> {
    @Parameter(names = "-start", required = true, description = "[Filter] Start date (inclusive), format == yyyy-MM-dd")
    protected volatile Date start;

    @Parameter(names = "-stop", required = false, description = "[Filter] Stop date (exclusive), format == yyyy-MM-dd")
    protected volatile Date stop;

    
    @Parameter(names = "-dummy", description = "dummy run (won't save view)")
    protected boolean dummy = true;

    protected int batchSize = 1000000;
    protected long startTime;
    protected long endTime;
    
    @SuppressWarnings("deprecation")
    @Override
    public void run(Injector arg0) throws Exception {
        super.run(arg0);
        try {
            setStartTime(System.currentTimeMillis());
            Iterable<AisPacket> iter = makeRequest();           
            
            for (AisPacket p : iter) {
                
                if (p != null) {
                    count.incrementAndGet();
                    this.accept(p);
                }

                if (count.get() % batchSize  == 0) {
                    long ms = System.currentTimeMillis() - startTime;
                    System.out.println(count.get() + " packets,  " + count.get()
                            / ((double) ms / 1000) + " packets/s");
                }
            }
            long ms = System.currentTimeMillis() - startTime;
            System.out.println("Total: " + count + " packets,  " + count.get()
                    / ((double) ms / 1000) + " packets/s");
            
            if (!dummy) {
                postProcess();
            }
            
            setEndTime(System.currentTimeMillis());
            
        } finally {
            con.stop();
            viewSession.shutdown();
        }
    }

    protected abstract Iterable<AisPacket> makeRequest();
    
    public boolean isDummy() {
        return dummy;
    }

    public long getStartTime() {
        return startTime;
    }
    
    public long getDuration() {
        return endTime-startTime;
    }
    
    public long getPacketsPerSecond() {
        try {
            return getCountValue()/(getDuration()/1000);
        } catch (ArithmeticException e) {
            return -1L;
        }
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
        String header = "getClass,getApplicationName,getStartTime,getEndTime,getDuration,getCountValue,getPacketsPerSecond";
        sb.append(header);
        sb.append("\n");

        for (String method: Arrays.asList(header.split(","))) {
            try {
                sb.append(this.getClass().getMethod(method).invoke(this));
            } catch (IllegalAccessException | IllegalArgumentException
                    | InvocationTargetException | NoSuchMethodException
                    | SecurityException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            sb.append(",");
        }
        sb.append("\n");
        
        return sb.toString();
    }

    /**
     * This optional step is run after a scan is completed
     */
    protected void postProcess() {
        
    }
    
    
    
    

}
