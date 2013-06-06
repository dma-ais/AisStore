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
package dk.dma.ais.store.web.rest;

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
import dk.dma.ais.store.web.rest.XStreamOutputStreamSink.OutputType;
import dk.dma.commons.util.io.OutputStreamSink;
import dk.dma.db.Query;
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
        return s == null ? ";" : s.get(0);
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
            q = mqs.findByMMSI(mmsi.iterator().next(), interval);
        } else if (area != null) {
            q = mqs.findByArea(area, interval);
        } else {
            throw new UnsupportedOperationException(
                    "Either a mmsinumber such as 'mmsi=123456789' must be specified. Or an area must be specified such as 'box=12.434,45.123,30.12,54.23) (lat1, lon1, lat2, lon2) ");
        }
        return createStreamingOutput(applyFilters(info, q), oss);
    }

    @GET
    @Path("raw")
    @Produces("text/plain")
    public StreamingOutput raw(@Context UriInfo info) {
        return execute(info, AisPackets.OUTPUT_TO_TEXT);
    }

    @GET
    @Path("rawsentences")
    @Produces("text/plain")
    public StreamingOutput rawsentences(@Context UriInfo info) {
        return execute(info, AisPackets.OUTPUT_PREFIXED_SENTENCES);
    }

    @GET
    @Path("xml")
    @Produces("text/plain")
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
    @Produces("text/plain")
    public StreamingOutput table(@Context UriInfo info, @QueryParam("columns") final String columns) {
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
