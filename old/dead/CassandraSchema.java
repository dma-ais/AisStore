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

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;

import dk.dma.ais.message.AisMessage;
import dk.dma.ais.message.IVesselPositionMessage;
import dk.dma.ais.packet.AisPacket;
import dk.dma.app.util.TimeUtil;
import dk.dma.enav.model.geometry.Position;

/**
 * 
 * @author Kasper Nielsen
 */
public abstract class CassandraSchema {
    protected CassandraSchema(String columnfamily) {

    }

    public abstract void write(MutationBatch m, AisPacket packet);

    protected void writeTimes(ColumnListMutation<String> row, long timestamp) {
        row.putColumn("timeday", TimeUtil.daysSinceEpoch(timestamp), null);
        row.putColumn("timehour", TimeUtil.hoursSinceEpoch(timestamp), null);
        row.putColumn("timeminute", TimeUtil.minutesSinceEpoch(timestamp), null);
    }

    protected void tryWritePositionCells(ColumnListMutation<String> row, AisMessage message) {
        if (message instanceof IVesselPositionMessage) {
            IVesselPositionMessage m = (IVesselPositionMessage) message;
            Position p = m.getPos().tryGetGeoLocation();
            if (p != null) {
                row.putColumn("cell001", p.getCell(0.01));
                row.putColumn("cell01", p.getCell(0.1));
                row.putColumn("cell1", p.getCell(1));
            }
        }
    }
}
