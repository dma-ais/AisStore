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
package dk.dma.ais.store.util;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 
 * @author Kasper Nielsen
 */
public class TimeUtil {

    public static Date substract(Date date, long durationToSubstract, TimeUnit unit) {
        long result = unit.toSeconds(durationToSubstract);
        if (result >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot substract " + durationToSubstract);
        }
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.add(Calendar.SECOND, -(int) result);
        return c.getTime();
    }

    public static int daysSinceEpoch(long epochTime) {
        return hoursSinceEpoch(epochTime) / 24;
    }

    public static int hoursSinceEpoch(long epochTime) {
        return minutesSinceEpoch(epochTime) / 60;
    }

    public static int minutesSinceEpoch(long epochTime) {
        return secondsSinceEpoch(epochTime) / 60;
    }

    public static int secondsSinceEpoch(long epochTime) {
        if (epochTime <= 0) {
            throw new IllegalArgumentException("epochTime must be positive, was " + epochTime);
        }
        long result = epochTime / 1000;
        if (result >= Integer.MAX_VALUE) {
            // sorry guys this will fail in 2037. Hope this code is not around anymore
            throw new IllegalArgumentException("epochtime was to high, epochTime = " + epochTime);
        }
        return (int) result;
    }
}
