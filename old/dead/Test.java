/*
 * Copyright (c) 2008 Kasper Nielsen.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dk.dma.ais.store.cassandra.schema;

import java.util.HashSet;

import dk.dma.enav.model.geometry.Position;

/**
 * 
 * @author Kasper Nielsen
 */
public class Test {

    public static void main(String[] args) {
        HashSet<Long> cellId = new HashSet<>();
        for (double i = -180; i <= 180; i += 1) {
            for (double j = -90; j <= 90; j += 1) {
                Position p = Position.create(j, i);
                cellId.add(p.getCell(10));
                if (j == 40) {
                    System.out.println(p + " " + p.getCell(10));
                }
            }
        }
        // System.out.println(65161 / 360);
        System.out.println(cellId.size());
    }
}
