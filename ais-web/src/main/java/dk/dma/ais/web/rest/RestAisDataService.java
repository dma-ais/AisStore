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
import dk.dma.ais.store.query.Query;
import dk.dma.ais.web.rest.XStreamOutputStreamSink.OutputType;
import dk.dma.commons.util.io.OutputStreamSink;
import dk.dma.enav.model.geometry.Area;
import dk.dma.enav.model.geometry.BoundingBox;
import dk.dma.enav.model.geometry.CoordinateSystem;
import dk.dma.enav.model.geometry.Position;

/**
 * 
 * @author Kasper Nielsen
 */
@Path("ais")
public class RestAisDataService extends AbstractRestService {

    /**
     * @throws Exception
     */
    public RestAisDataService() throws Exception {
        super();
    }

    private boolean writeHeader(UriInfo info) {
        return !info.getQueryParameters().containsKey("noHeader");
    }

    private Set<Integer> findMmsi(UriInfo info) {
        List<String> mmsi = info.getQueryParameters().get("mmsi");
        return convert(mmsi);
    }

    private String findSeperator(UriInfo info) {
        List<String> s = info.getQueryParameters().get("separator");
        if (s == null) {
            return ";";
        }
        return s.get(0);
    }

    private Area findArea(UriInfo info) {
        List<String> box = info.getQueryParameters().get("box");
        if (box != null) {
            if (box.size() > 1) {
                throw new UnsupportedOperationException("Only one box can be specified, was " + box);
            }
            String s = box.iterator().next();
            String[] str = s.split(",");
            if (str.length != 4) {
                throw new UnsupportedOperationException("A box must contain exactly 4 points, was " + str.length + "("
                        + s + ")");
            }
            double lat1 = Double.parseDouble(str[0]);
            double lon1 = Double.parseDouble(str[1]);
            double lat2 = Double.parseDouble(str[2]);
            double lon2 = Double.parseDouble(str[3]);
            Position p1 = Position.create(lat1, lon1);
            Position p2 = Position.create(lat2, lon2);
            return BoundingBox.create(p1, p2, CoordinateSystem.CARTESIAN);
        }
        return null;
    }

    private StreamingOutput execute(UriInfo info, OutputStreamSink<AisPacket> oss) {
        Set<Integer> mmsi = findMmsi(info);
        Interval interval = findInterval(info);
        Area area = findArea(info);
        Query<AisPacket> q;
        if (mmsi.size() >= 1) {
            q = mqs.findByMMSI(interval, mmsi.iterator().next());
        } else if (area != null) {
            q = mqs.findByArea(area, interval.getStart().toDate(), interval.getEnd().toDate());
        } else {
            throw new UnsupportedOperationException(
                    "Either a mmsinumber such as 'mmsi=123456789' must be specified. Or an area must be specified such as 'box=12.434,45.123,30.12,54.23) (lat1, lon1, lat2, lon2) ");
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
    @Path("rawsentences")
    @Produces("text/txt")
    public StreamingOutput rawsentences(@Context UriInfo info) {
        return execute(info, AisPackets.OUTPUT_PREFIXED_SENTENCES);
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
        return execute(info, new ReflectionBasedTableOutputStreamSink(columns, writeHeader(info), findSeperator(info)));
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
