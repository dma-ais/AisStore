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
package dk.dma.ais.store.old.exporter;

import static dk.dma.ais.store.AisStoreSchema.TABLE_TIME;
import static java.util.Objects.requireNonNull;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.tools.NodeCmd;

/**
 * Writing a snapshot takes a couple of tricks. Because someone decided it would be a good idea using System.exit(0) in
 * NodeCmd. We install a fake SecurityManager that catches whenever NodeCmd tries to exit with status code 0.
 * 
 * @author Kasper Nielsen
 */
public class CassandraNodeTool {

    private final String host;

    private final int port;

    public CassandraNodeTool() {
        this("localhost", 7199);
    }

    CassandraNodeTool(String host, int port) {
        this.host = requireNonNull(host);
        this.port = port;
    }

    public void deleteSnapshot(String keyspace, String snapshotName) throws Exception {
        execute("clearsnapshot", requireNonNull(keyspace), "-t", requireNonNull(snapshotName));
    }

    private void execute(String... args) throws Exception {
        List<String> list = new ArrayList<>(Arrays.asList("-h", host, "-p", "" + port));
        list.addAll(Arrays.asList(args));

        System.setSecurityManager(new NoExitSecurityManager());
        try {
            NodeCmd.main(list.toArray(new String[list.size()]));
        } catch (DummyException ok) {} finally {
            System.setSecurityManager(null);
        }
    }

    public void takeSnapshot(String keyspace, String snapshotName) throws Exception {
        execute("snapshot", requireNonNull(keyspace), "-cf", TABLE_TIME, "-t", requireNonNull(snapshotName));
    }
    
    public void takeSnapshot(String keyspace, String snapshotName, String tableName) throws Exception {
        execute("snapshot", requireNonNull(keyspace), "-cf", tableName, "-t", requireNonNull(snapshotName));
    }

    @SuppressWarnings("serial")
    static class DummyException extends SecurityException {}

    static class NoExitSecurityManager extends SecurityManager {
        @Override
        public void checkExit(int status) {
            if (status == 0) {// only throw if successful, in this way we exit on error conditions
                throw new DummyException();
            }
        }

        @Override
        public void checkPermission(Permission perm) {}

        @Override
        public void checkPermission(Permission perm, Object context) {}
    }
}
