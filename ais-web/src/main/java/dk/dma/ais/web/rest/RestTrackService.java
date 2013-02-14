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

import java.text.DecimalFormat;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import org.joda.time.Interval;
import org.joda.time.Period;

import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;

import dk.dma.ais.message.AisMessage;
import dk.dma.ais.message.AisPosition;
import dk.dma.ais.message.IVesselPositionMessage;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.query.Query;
import dk.dma.ais.web.rest.XStreamOutputStreamSink.OutputType;
import dk.dma.commons.util.io.OutputStreamSink;
import dk.dma.enav.model.geometry.Position;

/**
 * 
 * @author Kasper Nielsen
 */
@Path("/track")
public class RestTrackService extends AbstractRestService {

    public RestTrackService() throws Exception {
        super();
    }

    private StreamingOutput execute(int mmsi, UriInfo info, OutputStreamSink<AisPacket> oss) {
        Interval interval = findInterval(info);
        Query<AisPacket> q = mqs.findByMMSI(interval, mmsi);
        return createStreamingOutput(q, oss);
    }

    @GET
    @Path("{mmsi : \\d+}/json")
    @Produces("application/json")
    public StreamingOutput json(@PathParam("mmsi") int mmsi, @Context UriInfo info) {
        return execute(mmsi, info, new Sink(OutputType.JSON, info));
    }

    @GET
    @Path("{mmsi : \\d+}/xml")
    @Produces("text/xml")
    public StreamingOutput xml(@PathParam("mmsi") int mmsi, @Context UriInfo info) {
        return execute(mmsi, info, new Sink(OutputType.XML, info));
    }

    /** A sink that uses XStream to write out the data */
    static class Sink extends XStreamOutputStreamSink<AisPacket> {
        static final DecimalFormat df = new DecimalFormat("###.#####");

        Position lastPosition;
        Long lastTimestamp;
        final Long sampleDuration;
        final Integer samplePositions;

        Sink(XStreamOutputStreamSink.OutputType outputType, UriInfo info) {
            super(AisPacket.class, "track", "point", outputType);
            String sp = info.getQueryParameters().getFirst("minDistance");
            String dur = info.getQueryParameters().getFirst("minDuration");
            samplePositions = sp == null ? null : Integer.parseInt(sp);
            sampleDuration = dur == null ? null : Period.parse(dur).toStandardSeconds().getSeconds() * 1000L;
        }

        /** {@inheritDoc} */
        @Override
        public void write(AisPacket p, HierarchicalStreamWriter writer, MarshallingContext context) {
            AisMessage m = p.tryGetAisMessage();
            IVesselPositionMessage im = (IVesselPositionMessage) m;
            Position pos = im.getPos().getGeoLocation();
            lastPosition = pos;
            lastTimestamp = p.getBestTimestamp();
            w(writer, "timestamp", p.getBestTimestamp());
            w(writer, "lon", df.format(pos.getLongitude()));
            w(writer, "lat", df.format(pos.getLatitude()));
            w(writer, "sog", im.getSog());
            w(writer, "cog", im.getCog());
            w(writer, "heading", im.getTrueHeading());
        }

        public boolean isPacketWriteable(AisPacket packet) {
            AisMessage m = packet.tryGetAisMessage();
            if (m instanceof IVesselPositionMessage) {
                AisPosition a = ((IVesselPositionMessage) m).getPos();
                if (a != null) {
                    Position pos = a.getGeoLocation();
                    if (pos != null) {
                        if (sampleDuration == null && samplePositions == null) {
                            return true;
                        }
                        if (samplePositions != null
                                && (lastPosition == null || lastPosition.rhumbLineDistanceTo(pos) >= samplePositions)) {
                            return true;
                        }
                        return sampleDuration != null
                                && (lastTimestamp == null || packet.getBestTimestamp() - lastTimestamp >= sampleDuration);
                    }
                }
            }
            return false;
        }
    };
}
