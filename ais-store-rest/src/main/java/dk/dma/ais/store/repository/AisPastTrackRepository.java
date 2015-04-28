/* Copyright (c) 2011 Danish Maritime Authority.
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
package dk.dma.ais.store.repository;

import dk.dma.ais.message.IPositionMessage;
import dk.dma.ais.packet.AisPacketSource;
import dk.dma.ais.store.AisStoreQueryBuilder;
import dk.dma.ais.store.AisStoreQueryResult;
import dk.dma.db.cassandra.CassandraConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 *
 * @author Thomas Borg Salling <tbsalling@tbsalling.dk>
 */
@ThreadSafe
@Repository
public class AisPastTrackRepository {

    private static final Logger LOG = LoggerFactory.getLogger(AisPastTrackRepository.class);
    { LOG.info("AisPastTrackRepository created.");  }

    @Inject
    private CassandraConnection cassandraConnection;

    /**
     * Query the database for packets from the supplied mmsi numbers and with transmission
     * timestamps at or after t0 and at or before t1.
     *
     * Intended for "small" queries where the query result can be kept in memory.
     *
     * @param sourceFilterPredicate
     * @param t0
     * @param t1
     * @param mmsi
     * @return
     */
    public List<IPositionMessage> findByMmsi(Predicate<AisPacketSource> sourceFilterPredicate, Instant t0, Instant t1, int mmsi) {

        AisStoreQueryBuilder query = AisStoreQueryBuilder
            .forMmsi(mmsi)
            .setInterval(t0, t1)
            .setFetchSize(1000);

        AisStoreQueryResult result = cassandraConnection.execute(query);

        List<IPositionMessage> pastPositionMessages = StreamSupport.stream(result.spliterator(), false)
            .filter(packet -> sourceFilterPredicate.test(AisPacketSource.create(packet)))
            .map(packet -> packet.tryGetAisMessage())
            .filter(msg -> msg instanceof IPositionMessage)
            .map(msg -> (IPositionMessage) msg)
            .collect(Collectors.toList());

        return pastPositionMessages;

    }

    /**
     * Query the database for packets from the supplied mmsi numbers and with transmission
     * timestamps not older than maxAge.
     *
     *
     * @param sourceFilterPredicate
     * @param maxAge
     * @param mmsi
     * @return
     */
    public List<IPositionMessage> findByMmsi(Predicate<AisPacketSource> sourceFilterPredicate, Duration maxAge, int mmsi) {
        return findByMmsi(sourceFilterPredicate, Instant.now().minus(maxAge), Instant.now(),  mmsi);
    }

}
