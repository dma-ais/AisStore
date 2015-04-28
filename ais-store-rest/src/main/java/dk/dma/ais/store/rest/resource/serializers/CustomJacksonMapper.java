package dk.dma.ais.store.rest.resource.serializers;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import dk.dma.ais.message.IVesselPositionMessage;
import dk.dma.ais.packet.AisPacketSource;
import dk.dma.enav.model.geometry.Position;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
@Qualifier(value = "objectMapper")
public class CustomJacksonMapper extends ObjectMapper {

    public CustomJacksonMapper() {
        setSerializationInclusion(JsonInclude.Include.NON_NULL);

        SimpleModule module = new SimpleModule();
        module.addSerializer(AisPacketSource.class, new AisPacketSourceSerializer());
       // module.addSerializer(AisMessage18.class, new IPositionMessageSerializer());
        module.addSerializer(IVesselPositionMessage.class, new IVesselPositionMessageSerializer());
        module.addSerializer(Position.class, new PositionSerializer());
        module.addSerializer(Date.class, new DateSerializer());
        this.registerModule(module);
    }

}
