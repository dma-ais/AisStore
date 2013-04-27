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
package dk.dma.ais.store;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import dk.dma.ais.store.util.TimeUtil;

/**
 * 
 * @author Kasper Nielsen
 */
public class AisQueries {

    public static Interval toInterval(String isoXXInterval) {
        if (!isoXXInterval.contains("/")) {
            isoXXInterval += "/" + DateTime.now();
        }
        return Interval.parse(isoXXInterval);
    }

    public static Interval toInterval(Date startDate, Date endDate) {
        return new Interval(startDate.getTime(), endDate.getTime());
    }

    public static Interval toInterval(long timeback, TimeUnit unit) {
        Date now = new Date();
        return toInterval(TimeUtil.substract(now, timeback, unit), now);
    }
}
