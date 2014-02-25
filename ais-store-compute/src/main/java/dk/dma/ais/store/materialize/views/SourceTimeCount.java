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

import dk.dma.ais.packet.AisPacket;
import dk.dma.ais.store.materialize.util.TypeSafeMapOfMaps;
import dk.dma.ais.store.materialize.util.TypeSafeMapOfMaps.Key2;
import dk.dma.enav.util.function.Consumer;

public class SourceTimeCount implements Consumer<AisPacket> {
    TypeSafeMapOfMaps<Key2<String, String>, Long> data = new TypeSafeMapOfMaps<>();
    
    private SimpleDateFormat timeFormatter;
    
    @Override
    public void accept(AisPacket aisPacket) {
        Objects.requireNonNull(aisPacket);
        String source = Objects.requireNonNull(aisPacket.getTags().getSourceId());
        Long timestamp = aisPacket.getBestTimestamp();
        
        if (timestamp > 0) {
            String time = Objects.requireNonNull(timeFormatter.format(new Date(timestamp)));
            try {
                Long value = data.get(TypeSafeMapOfMaps.key(source, time));
                data.put(TypeSafeMapOfMaps.key(source, time),value+1);
            } catch (NullPointerException e) {
                data.put(TypeSafeMapOfMaps.key(source, time),0L);
            }
            
        }
    }


    public TypeSafeMapOfMaps<Key2<String, String>, Long> getData() {
        return data;
    }

}
