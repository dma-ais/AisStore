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
package dk.dma.ais.store.util;

import java.util.Arrays;

import dk.dma.enav.model.geometry.Position;

/**
 * 
 * @author Kasper Nielsen
 */
public enum CellResolution {
    CELL10(10), CELL1(1), CELL01(0.1), CELL001(0.01);
    private final double degress;

    CellResolution(double degress) {
        this.degress = degress;
    }

    public double getDegress() {
        return degress;
    }

    public int getCell(Position position) {
        // bigger cellsize than 0.01 cannot be supported. unless we change the cellsize to long
        return (int) position.getCell(degress);
    }

    public void checkAnyOf(CellResolution... resolutions) {
        for (CellResolution r : resolutions) {
            if (this == r) {
                return;
            }
        }
        throw new IllegalArgumentException("Expected one of " + Arrays.toString(resolutions) + ", but was " + this);
    }

}