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
package dk.dma.ais.store.export;

import java.security.Permission;

import org.apache.cassandra.tools.NodeCmd;

import dk.dma.ais.store.cassandra.Tables;

/**
 * Writing a snapshot takes a couple of tricks. Because someone decided it would be a good idea using System.exit(0) in
 * NodeCmd. We install a fake SecurityManager that catches whenever NodeCmd tries to exit with status code 0.
 * 
 * @author Kasper Nielsen
 */
class CassandraSnapshotWriter {

    static void takeSnapshot(String keyspace, String snapshotName) throws Exception {
        System.setSecurityManager(new NoExitSecurityManager());
        try {
            NodeCmd.main(new String[] { "-h", "localhost", "-p", "7199", "snapshot", keyspace, "-cf",
                    Tables.AIS_MESSAGES, "-t", snapshotName });
        } catch (DummyException ok) {} finally {
            System.setSecurityManager(null);
        }
    }

    static void deleteSnapshot(String keyspace, String snapshotName) throws Exception {
        System.setSecurityManager(new NoExitSecurityManager());
        try {
            NodeCmd.main(new String[] { "-h", "localhost", "-p", "7199", "clearsnapshot", keyspace, "-t", snapshotName });
        } catch (DummyException ok) {} finally {
            System.setSecurityManager(null);
        }
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
