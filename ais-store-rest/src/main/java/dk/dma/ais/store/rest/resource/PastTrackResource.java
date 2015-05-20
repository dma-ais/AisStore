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

package dk.dma.ais.store.rest.resource;

import dk.dma.ais.message.IPositionMessage;
import dk.dma.ais.packet.AisPacketSource;
import dk.dma.ais.packet.AisPacketSourceFilters;
import dk.dma.ais.store.repository.AisPastTrackRepository;
import dk.dma.ais.store.rest.resource.exceptions.CannotParseFilterExpressionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * @author tbsalling
 */
@RestController
public class PastTrackResource {

    private static final Logger LOG = LoggerFactory.getLogger(PastTrackResource.class);
    { LOG.info("TrackResource created."); }

    @Inject
    private AisPastTrackRepository pastTrackRepository;

    @PostConstruct
    private void init() {
    }

    private final Instant timeStarted = Instant.now();

    /**
     * Show status page.
     *
     * Example URL:
     * - http://localhost:8080/?sourceFilter=s.country%20in%20(DK)
     *
     * @param sourceFilterExpression
     * @return
     */
    @RequestMapping(value="/", produces = MediaType.TEXT_PLAIN_VALUE)
    String home(@RequestParam(value="sourceFilter", required = false) String sourceFilterExpression) {
        StringBuilder sb = new StringBuilder();

        sb.append("Danish Maritime Authority - AisStore REST service\n")
          .append("-------------------------------------------------\n")
          .append("\n");

        return sb.toString();
    }


    /**
     */
    @RequestMapping(value = "/pastTrack/{mmsi}", produces = MediaType.APPLICATION_JSON_VALUE)
    List<IPositionMessage> track(@PathVariable int mmsi, @RequestParam(value = "sourceFilter", required = false) String sourceFilterExpression, @RequestParam(value = "duration", required = true) String iso8601Duration) {
        Objects.requireNonNull(iso8601Duration);
        List<IPositionMessage> pastTrack = pastTrackRepository.findByMmsi(createSourceFilterPredicate(sourceFilterExpression), Duration.parse(iso8601Duration), mmsi);
        LOG.debug("Found " + pastTrack.size() + " past track entries for MMSI " + mmsi);
        return pastTrack;
    }

    /** Create a Predicate<AisPacketSource> out of a user supplied expression string */
    static Predicate<AisPacketSource> createSourceFilterPredicate(String sourceFilterExpression) {
        Predicate<AisPacketSource> sourceFilter;

        if (! isBlank(sourceFilterExpression)) {
            try {
                sourceFilter = AisPacketSourceFilters.parseSourceFilter(sourceFilterExpression);
            } catch (Exception e) {
                throw new CannotParseFilterExpressionException(e, sourceFilterExpression);
            }
        } else {
            sourceFilter = src -> true;
        }

        return sourceFilter;
    }

}
