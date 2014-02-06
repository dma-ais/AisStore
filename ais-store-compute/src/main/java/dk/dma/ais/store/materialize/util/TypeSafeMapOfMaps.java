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

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A multi-key map that is typesafe, uses natural ordering. Convenient normal map iterator
 *
 * @param <K>
 * @param <V>
 */
public class TypeSafeMapOfMaps<K extends Comparable<K>, V> implements Iterable<Map.Entry<K, V>> {
    private final ConcurrentSkipListMap<K, V> map = new ConcurrentSkipListMap<>();

    public V get(K key) {
        return map.get(key);
    }

    /** {@inheritDoc} */
    @Override
    public Iterator<Entry<K, V>> iterator() {
        return map.entrySet().iterator();
    }

    public TypeSafeMapOfMaps<K, V> put(K key, V value) {
        map.put(key, value);
        return this;
    }

    public static <K1 extends Comparable<K1>, K2 extends Comparable<K2>> Key2<K1, K2> key(K1 k1, K2 k2) {
        return new Key2<>(k1, k2);
    }
    
    public static <K1 extends Comparable<K1>, K2 extends Comparable<K2>, K3 extends Comparable<K3>> Key3<K1, K2, K3> key3(K1 k1, K2 k2, K3 k3) {
        return new Key3<K1, K2, K3>(k1, k2, k3);
    }

    public static <K1 extends Comparable<K1>, K2 extends Comparable<K2>, K3 extends Comparable<K3>> Key3<K1, K2, K3> key(
            K1 k1, K2 k2, K3 k3) {
        return new Key3<>(k1, k2, k3);
    }

    public static <K1 extends Comparable<K1>, K2 extends Comparable<K2>, K3 extends Comparable<K3>, K4 extends Comparable<K4>> Key4<K1, K2, K3, K4> key(
            K1 k1, K2 k2, K3 k3, K4 k4) {
        return new Key4<>(k1, k2, k3, k4);
    }

    public static void main(String[] args) {
        TypeSafeMapOfMaps<Key2<Long, String>, Boolean> map = new TypeSafeMapOfMaps<>();

        map.put(key(123L, "fff"), Boolean.TRUE);
        map.put(key(23L, "fff"), Boolean.TRUE);
        map.put(key(2443L, "fff"), Boolean.TRUE);
        map.put(key(123L, "aff"), Boolean.TRUE);

        for (Entry<Key2<Long, String>, Boolean> e : map) {
            System.out.println(e.getKey().getK1());
        }
    }

    public static class Key2<K1 extends Comparable<K1>, K2 extends Comparable<K2>> implements Comparable<Key2<K1, K2>> {

        final K1 k1;

        final K2 k2;

        public Key2(K1 k1, K2 k2) {
            this.k1 = requireNonNull(k1);
            this.k2 = requireNonNull(k2);
        }

        /** {@inheritDoc} */
        @Override
        public int compareTo(Key2<K1, K2> o) {
            int k = k1.compareTo(o.k1);
            if (k == 0) {
                return k2.compareTo(o.k2);
            }
            return k;
        }

        /**
         * @return the k1
         */
        public K1 getK1() {
            return k1;
        }

        /**
         * @return the k2
         */
        public K2 getK2() {
            return k2;
        }

        public String toString() {
            return "[" + k1 + "," + k2 + "]";
        }
    }

    public static class Key3<K1 extends Comparable<K1>, K2 extends Comparable<K2>, K3 extends Comparable<K3>>
            implements Comparable<Key3<K1, K2, K3>> {

        final K1 k1;

        final K2 k2;

        final K3 k3;

        public Key3(K1 k1, K2 k2, K3 k3) {
            this.k1 = requireNonNull(k1);
            this.k2 = requireNonNull(k2);
            this.k3 = requireNonNull(k3);
        }

        /** {@inheritDoc} */
        @Override
        public int compareTo(Key3<K1, K2, K3> o) {
            int k = k1.compareTo(o.k1);
            if (k == 0) {
                k = k2.compareTo(o.k2);
                if (k == 0) {
                    return k3.compareTo(o.k3);
                }
            }
            return k;
        }

        /**
         * @return the k1
         */
        public K1 getK1() {
            return k1;
        }

        /**
         * @return the k2
         */
        public K2 getK2() {
            return k2;
        }

        /**
         * @return the k3
         */
        public K3 getK3() {
            return k3;
        }

        public String toString() {
            return "[" + k1 + "," + k2 + "," + k3 + "]";
        }
    }

    public static class Key4<K1 extends Comparable<K1>, K2 extends Comparable<K2>, K3 extends Comparable<K3>, K4 extends Comparable<K4>>
            implements Comparable<Key4<K1, K2, K3, K4>> {

        final K1 k1;

        final K2 k2;

        final K3 k3;

        final K4 k4;

        public Key4(K1 k1, K2 k2, K3 k3, K4 k4) {
            this.k1 = requireNonNull(k1);
            this.k2 = requireNonNull(k2);
            this.k3 = requireNonNull(k3);
            this.k4 = requireNonNull(k4);
        }

        /** {@inheritDoc} */
        @Override
        public int compareTo(Key4<K1, K2, K3, K4> o) {
            int k = k1.compareTo(o.k1);
            if (k == 0) {
                k = k2.compareTo(o.k2);
                if (k == 0) {
                    k = k3.compareTo(o.k3);
                    if (k == 0) {
                        return k4.compareTo(o.k4);
                    }
                }
            }
            return k;
        }

        /**
         * @return the k1
         */
        public K1 getK1() {
            return k1;
        }

        /**
         * @return the k2
         */
        public K2 getK2() {
            return k2;
        }

        /**
         * @return the k3
         */
        public K3 getK3() {
            return k3;
        }

        /**
         * @return the k4
         */
        public K4 getK4() {
            return k4;
        }

        public String toString() {
            return "[" + k1 + "," + k2 + "," + k3 + "," + k3 + "]";
        }
    }
}
