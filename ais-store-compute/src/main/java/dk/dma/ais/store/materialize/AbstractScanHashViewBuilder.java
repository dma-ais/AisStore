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
package dk.dma.ais.store.materialize;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;

public abstract class AbstractScanHashViewBuilder extends Scan implements HashViewBuilder {
    private static Logger LOG = Logger.getLogger(AbstractScanHashViewBuilder.class);
    
    Integer batchSize = 1000;
    List<RegularStatement> batch = new ArrayList<>(batchSize*2);

    /**
     *  Force implementation/requirement of this (normally optional) step
     */
    @Override
    public abstract void postProcess();
    
}
