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
package dk.dma.ais.store.materialize.test;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;

import com.beust.jcommander.Parameter;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.google.inject.Injector;

import dk.dma.ais.store.materialize.AisMatSchema;
import dk.dma.ais.store.materialize.cli.AbstractViewCommandLineTool;

public class AisMatQueryTest extends AbstractViewCommandLineTool {
    
    @Parameter(names = "-start", required = true, description = "[Filter] Start date (inclusive), format == yyyy-MM-dd")
    protected volatile Date start;
    
    
    SimpleDateFormat sdf = new SimpleDateFormat(AisMatSchema.HOUR_FORMAT);
    
    @Override
    public void run(Injector arg0) throws Exception {
        //super.run(arg0);
        
        
        Selection qb = QueryBuilder.select();
        
        LinkedList<String> hours = new LinkedList<>();
        Calendar c = Calendar.getInstance();
        c.setTime(start);
        for (int i = 0; i < 24; i ++) {
            hours.add(sdf.format(c.getTime()));
            c.add(Calendar.HOUR_OF_DAY,1);
        }
       
        
        ResultSet rs = viewSession.execute(
                qb.all().
                from(AisMatSchema.TABLE_MMSI_TIME_COUNT).
                where(QueryBuilder.eq(AisMatSchema.MMSI_KEY, 219000174)).
                and(QueryBuilder.in(AisMatSchema.TIME_KEY, hours)));
        
        for (Row r: rs) {
            System.out.print(r.getString(AisMatSchema.TIME_KEY)+":"+r.getInt(AisMatSchema.VALUE)+",");
        }
    }

    public static void main(String[] args) throws Exception {
        AisMatQueryTest amqt = new AisMatQueryTest();
        amqt.execute(args);
    }
}
