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

}
