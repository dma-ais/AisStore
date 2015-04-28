package dk.dma.ais.store.rest.resource.serializers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import dk.dma.ais.message.AisMessage;
import dk.dma.ais.message.IVesselPositionMessage;
import dk.dma.ais.proprietary.IProprietarySourceTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class IVesselPositionMessageSerializer extends JsonSerializer<IVesselPositionMessage> {

    private static final Logger LOG = LoggerFactory.getLogger(IVesselPositionMessageSerializer.class);

    @Override
    public void serialize(IVesselPositionMessage pos, JsonGenerator jg, SerializerProvider serializerProvider) throws IOException, JsonProcessingException {
        jg.useDefaultPrettyPrinter();

        jg.writeStartObject();

        if (pos instanceof AisMessage) {
            IProprietarySourceTag src = ((AisMessage) pos).getSourceTag();
            if (src != null) {
                jg.writeObjectField("src.clk", src.getTimestamp());
                jg.writeObjectField("src.id", src.getBaseMmsi());
                jg.writeObjectField("src.reg", src.getRegion());
                if (src.getCountry() != null)
                    jg.writeObjectField("src.cty", src.getCountry().getTwoLetter());
            }
        }

        jg.writeObjectField("lat", pos.getPos().getGeoLocation().getLatitude());
        jg.writeObjectField("lon", pos.getPos().getGeoLocation().getLongitude());
        jg.writeObjectField("acc", pos.getPosAcc());
        jg.writeObjectField("hdg", pos.getTrueHeading());
        jg.writeObjectField("sog", pos.getSog()/10f);
        jg.writeObjectField("cog", pos.getCog()/10f);
        jg.writeEndObject();
    }
}
