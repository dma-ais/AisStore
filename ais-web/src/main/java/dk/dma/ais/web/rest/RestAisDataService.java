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
package dk.dma.ais.web.rest;

import java.util.List;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import org.joda.time.Interval;

import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

import dk.dma.ais.message.AisMessage;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.packet.AisPackets;
import dk.dma.ais.web.rest.XStreamOutputStreamSink.OutputType;
import dk.dma.app.cassandra.Query;
import dk.dma.app.io.OutputStreamSink;

/**
 * 
 * @author Kasper Nielsen
 */
@Path("aisdata")
public class RestAisDataService extends AbstractRestService {

    /**
     * @throws Exception
     */
    public RestAisDataService() throws Exception {
        super();
    }

    private Set<Integer> findMmsi(UriInfo info) {
        List<String> mmsi = info.getQueryParameters().get("mmsi");
        return convert(mmsi);
    }

    private StreamingOutput execute(UriInfo info, OutputStreamSink<AisPacket> oss) {
        Set<Integer> mmsi = findMmsi(info);
        Interval interval = findInterval(info);
        Query<AisPacket> q;
        if (mmsi.size() == 0) {
            q = mqs.findByShape(null, interval.getStart().toDate(), interval.getEnd().toDate());
        } else {
            q = mqs.findByMMSI(interval, mmsi.iterator().next());
        }
        return createStreamingOutput(applyFilters(info, q), oss);
    }

    @GET
    @Path("raw")
    @Produces("text/txt")
    public StreamingOutput raw(@Context UriInfo info) {
        return execute(info, AisPackets.OUTPUT_TO_TEXT);
    }

    @GET
    @Path("xml")
    @Produces("text/xml")
    public StreamingOutput xml(@Context UriInfo info) {
        return execute(info, new Sink(OutputType.XML));
    }

    @GET
    @Path("json")
    @Produces("application/json")
    public StreamingOutput json(@Context UriInfo info) {
        return execute(info, new Sink(OutputType.JSON));
    }

    @GET
    @Path("table")
    @Produces("text/txt")
    public StreamingOutput table(@Context UriInfo info, final @QueryParam("columns") String columns) {
        return execute(info, new ReflectionBasedTableOutputStreamSink(columns));
    }

    static class Sink extends XStreamOutputStreamSink<AisPacket> {

        Sink(XStreamOutputStreamSink.OutputType outputType) {
            super(AisPacket.class, "packets", "packet", outputType);
        }

        /** {@inheritDoc} */
        @Override
        public void write(AisPacket p, HierarchicalStreamWriter writer, MarshallingContext context) {
            writer.startNode("timestamp");
            writer.setValue(Long.toString(p.getBestTimestamp()));
            writer.endNode();
            AisMessage m = p.tryGetAisMessage();
            if (m != null) {
                writer.startNode("message");
                context.convertAnother(m);
                writer.endNode();
            }
        }
    }
}
