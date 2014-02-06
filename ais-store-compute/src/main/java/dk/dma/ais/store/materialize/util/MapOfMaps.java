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
package dk.dma.ais.store.materialize.util;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;
/**
 * A recursive map of maps for multi key data storage, type-unsafe but supports any number of keys.
 * 
 * @author Jens Tuxen
 *
 */
public class MapOfMaps {  
    Object k;
    ConcurrentSkipListMap<Object, MapOfMaps> map;
    Object v;
    
    boolean last;
    
    public MapOfMaps(Object v2,Object key) {
        this.k = key;
        this.v = v2;
        this.last = true;
    }
    
    public MapOfMaps(Object v, Object... keys) {
        if (keys.length == 1) {
            this.v = v;
            this.k = keys[0];
            this.last = true;
        } else {
            this.k = head(keys);
            this.map = new ConcurrentSkipListMap<>();
            this.map.put(k, new MapOfMaps(v,tail(keys)));
        }
    }  
    
    public ConcurrentSkipListMap<Object, MapOfMaps> getMap() {
        return map;
    }

    public void setMap(ConcurrentSkipListMap<Object, MapOfMaps> map) {
        this.map = map;
    }

    public Object getValue(Object... keys) {        
        MapOfMaps m = this;
       
        for (Object k: keys) {
            if (m.isLast()) {
                return m.get(k);
            }
            m = m.getMap().get(k);
            
        }
        
        return m.get(k);
    }
    
    private Object get(Object key) {
        return v;
    
    }

    public boolean containsValue(Object... keys) {
        return getValue(keys) != null;
    }
    
    public boolean isLast() {
        return last;
    }

    public void setLast(boolean last) {
        this.last = last;
    }
    
    public Object getK() {
        return k;
    }

    public void setK(Object keys) {
        this.k = keys;
    }

    public Object getV() {
        return v;
    }

    public void setV(Object v) {
        this.v = v;
    }
    
    private Object head(Object... keys) {
        return keys[0];
    }
    
    private Object[] tail(Object... keys) {
        Object[] t = Arrays.copyOfRange(keys, 1, keys.length); 
        return t;
    }
    
    @Override
    public String toString() {
        
        String vString = v == null ? "" : "v = "+v;
        //String kString = k == null ? "" : ",k = "+k;
        String mapString = map == null ? "" : "map = "+map;
        return "MapOfMaps ["+mapString +  vString + "]";
    }

    public void setValue(Object v, Object... keys) {
        if (keys.length == 1) {
            this.setLast(true);
            this.setV(v);
            this.setK(keys[0]);
            
        } else if (map.containsKey(head(keys))) {
            map.get(head(keys)).setValue(v,tail(keys));
        } else {
            map.put(head(keys),new MapOfMaps(v,tail(keys)));
        }
        
    }
    
    public static void main(String[] args) {
        MapOfMaps m = new MapOfMaps(10L,138947L, "moo");
        
        m.setValue(2L, 3L,"moo3");
        m.setValue(7L, 2L,"moo2");
        m.setValue(3L, 2L,"moo2");
        m.setValue(3L, 1L,"moo2");
        m.setValue(2L, 3L,"moo3");        
        
        System.out.println(m);
             
        for (Entry<Object, MapOfMaps> e: m.getMap().entrySet()) {
            Long mmsi = (Long) e.getKey();
            String day = (String) e.getValue().getK();
            Long value = (Long) e.getValue().getV();
            
            
            System.out.println(mmsi+" "+day+" "+value);
        }

    }
}
    




