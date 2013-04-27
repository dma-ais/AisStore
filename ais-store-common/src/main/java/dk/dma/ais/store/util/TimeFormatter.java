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

import java.sql.Date;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.utils.Hex;

import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;

/**
 * 
 * @author Kasper Nielsen
 */
public enum TimeFormatter {
    DAY100(100 * 24 * 60 * 60 * 1000), DAY(24 * 60 * 60 * 1000), HOUR(60 * 60 * 1000), MIN10(10 * 60 * 1000), MIN(
            60 * 1000), SECOND(1000);

    private final int divisor;

    TimeFormatter(int divisor) {
        this.divisor = divisor;
    }

    public long get(Date date) {
        return get(date.getTime());
    }

    public long get(long timestamp) {
        return timestamp / divisor;
    }

    public int getAsInt(long timestamp) {
        return Ints.checkedCast(timestamp / divisor);
    }

    public byte getReminderByteTo(long timestamp, TimeFormatter other) {
        if (other.divisor >= divisor) {
            throw new IllegalArgumentException("This = " + this + ", other " + other);
        }
        return UnsignedBytes.checkedCast(other.get(timestamp) % divisor);
    }

    public long fromCompactReminder(long prefix, byte[] reminder) {
        byte[] r = Bytes.concat(new byte[8 - reminder.length], reminder);
        return prefix * divisor + Longs.fromByteArray(r);
    }

    public int getReminderAsInt(long timestamp) {
        return Ints.checkedCast(timestamp % divisor);
    }

    public byte[] getCompactReminder(long timestamp) {
        long l = timestamp % divisor;
        byte[] b = Longs.toByteArray(l);
        return Arrays.copyOfRange(b, Long.numberOfLeadingZeros(timestamp) >> 3, 8);
    }

    public static void main(String[] args) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        for (TimeFormatter f : TimeFormatter.values()) {
            for (int i = 0; i < 100; i++) {
                long v = r.nextLong(1L << 62L);
                long l = f.get(v);
                byte[] b = f.getCompactReminder(v);

                System.out.println(v);
                System.out.println(f.fromCompactReminder(l, b));
            }
        }

        System.out.println(MIN.get(1523423423L));

        System.out.println();
        // System.out.println(MIN.fromCompactReminder(prefix, reminder)(1523423423L));
        System.out.println(Hex.bytesToHex(MIN.getCompactReminder(1523423423L)));
        System.out.println("bye");
    }
}
