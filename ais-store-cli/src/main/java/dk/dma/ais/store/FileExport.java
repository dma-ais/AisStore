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
package dk.dma.ais.store;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;

import com.beust.jcommander.Parameter;
import com.google.inject.Injector;

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.packet.AisPacketFilters;
import dk.dma.ais.packet.AisPacketOutputSinks;
import dk.dma.commons.app.AbstractCommandLineTool;
import dk.dma.commons.util.DateTimeUtil;
import dk.dma.commons.util.Iterables;
import dk.dma.commons.util.io.OutputStreamSink;
import dk.dma.db.cassandra.CassandraConnection;
import dk.dma.enav.model.geometry.BoundingBox;
import dk.dma.enav.model.geometry.CoordinateSystem;
import dk.dma.enav.model.geometry.Position;

/**
 * 
 * @author Kasper Nielsen
 * @author Jens Tuxen
 */
public class FileExport extends AbstractCommandLineTool {

    @Parameter(names = "-keyspace", description = "The keyspace in cassandra")
    String keyspace = "aisdata";
    
    @Parameter(names = "-seeds", description = "List of Cassandra nodes (minimum one is needed)")
    ArrayList<String> seeds = new ArrayList<String>();

    @Parameter(names = "-filter", description = "The filter to apply")
    String filter;

    @Parameter(names = "-interval", description = "The ISO 8601 time interval for data to export")
    String interval;
    
    @Parameter(names = "-mmsi", description = "Extract from mmsi schema")
    List<Integer> mmsis = new ArrayList<Integer>();
    
    @Parameter(names = "-area", description = "Extract from geopgraphic cells schema")
    String area;
    
    @Parameter(names = "-outputFormat", description = "Output format, options: raw, json, jsonObject (use -columns), kml, kmz, table (use -columns)")
    String outputFormat = "raw";
    
    @Parameter(names = "-columns", description = "Optional columns, used for jsonObject and table.")
    String columns;
    
    @Parameter(names = "-seperator", description = "Optional seperator, used for table format.")
    String seperator = ",";
    
    @Parameter(names = "-file", description = "File to extract to (default is stdout)")
    String filePath;

    @Parameter(names = "-fetchSize", description = "internal fetch size buffer")
    Integer fetchSize = 3000;
    

    /** {@inheritDoc} */
    @Override
    protected void run(Injector injector) throws Exception {
        AisStoreQueryBuilder b;
        if (!mmsis.isEmpty()) {
            b = AisStoreQueryBuilder.forMmsi(ArrayUtils.toPrimitive(mmsis.toArray(new Integer[0])));  
            b.setFetchSize(fetchSize);
        } else if (area != null) {            
            BoundingBox bbox = findBoundingBox(area);
            b = AisStoreQueryBuilder.forArea(bbox);
            b.setFetchSize(fetchSize);
        } else {
            b = AisStoreQueryBuilder.forTime();
            b.setFetchSize(fetchSize);
        }
        
        b.setInterval(DateTimeUtil.toInterval(interval));
        
        CassandraConnection conn = CassandraConnection.create(keyspace, seeds);
        conn.startAsync();
        
        AisStoreQueryResult result = conn.execute(b);
        Iterable<AisPacket> iterableResult = result;
        
        if (filter != null) {
            iterableResult = Iterables.filter(iterableResult, AisPacketFilters.parseExpressionFilter(filter));
        }
        
        OutputStreamSink<AisPacket> sink = AisPacketOutputSinks.getOutputSink(outputFormat,columns,seperator);
        
        FileOutputStream fos;
        if (filePath != null) {
            fos  = new FileOutputStream(new File(filePath));
        } else {
            fos = new FileOutputStream(FileDescriptor.out);
        }
        
        sink.closeWhenFooterWritten();       
        sink.writeAll(iterableResult, fos);               
        conn.stopAsync();
    }
    
    private BoundingBox findBoundingBox(String s) {
        String[] arr = s.split(",");
        
        if (arr.length != 4) {
            return null;
        }
        
        Double[] coords = new Double[4];
        for (int i=0; i<4; i++) {
            coords[i] = Double.parseDouble(arr[i]);
        }
        
        return BoundingBox.create(Position.create(coords[0], coords[1]), Position.create(coords[2],coords[3]), CoordinateSystem.CARTESIAN);
    }

    public static void main(String[] args) throws Exception {
        new FileExport().execute(args);
    }
}
