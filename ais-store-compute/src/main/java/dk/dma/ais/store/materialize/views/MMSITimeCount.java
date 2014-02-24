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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

import dk.dma.ais.binary.SixbitException;
import dk.dma.ais.message.AisMessageException;
import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.materialize.util.TypeSafeMapOfMaps;
import dk.dma.ais.store.materialize.util.TypeSafeMapOfMaps.Key2;
import dk.dma.enav.util.function.Consumer;

public class MMSITimeCount implements Consumer<AisPacket> {
    TypeSafeMapOfMaps<Key2<Long, String>, Long> data = new TypeSafeMapOfMaps<>();
    private SimpleDateFormat timeFormatter;
    
    public MMSITimeCount(SimpleDateFormat timeFormatter) {
        this.timeFormatter = timeFormatter;
        
    }
    

    @Override
    public void accept(AisPacket aisPacket) {
        try {
            Objects.requireNonNull(aisPacket);
            Long mmsi = (long)Objects.requireNonNull(aisPacket.getAisMessage().getUserId());
            Long timestamp = aisPacket.getBestTimestamp();
            
            if (timestamp > 0) {
                String time = Objects.requireNonNull(timeFormatter.format(new Date(timestamp)));
                try {
                    Long value = data.get(TypeSafeMapOfMaps.key(mmsi, time));
                    data.put(TypeSafeMapOfMaps.key(mmsi, time),value+1);
                } catch (Exception e) {
                    data.put(TypeSafeMapOfMaps.key(mmsi, time),0L);
                }
                
            }

        } catch (AisMessageException | SixbitException e) {
            //e.printStackTrace();
        }
    }


    public TypeSafeMapOfMaps<Key2<Long, String>, Long> getData() {
        return data;
    }
    
}
