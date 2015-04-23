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
package dk.dma.ais.store.write;

import dk.dma.ais.tracker.targetTracker.TargetTracker;

/**
 * We wrap a packet using this class. The main issue is the target tracker. Which keeps the latest position. However,
 * since we share the packet queue between the backup process If we start restoring data from the backup the latest
 * timestamp will be overridden
 * 
 * @author Kasper Nielsen
 */
public class WrappedPacket {
    TargetTracker tracker;
}
