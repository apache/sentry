/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.tests.e2e.kafka;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

public class EmbeddedZkServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedZkServer.class);

    private Path snapshotDir = null;
    private Path logDir = null;
    private ZooKeeperServer zookeeper = null;
    private NIOServerCnxnFactory factory = null;

    public EmbeddedZkServer(int port) throws Exception {
        snapshotDir = Files.createTempDirectory("zookeeper-snapshot-");
        logDir = Files.createTempDirectory("zookeeper-log-");
        int tickTime = 500;
        zookeeper = new ZooKeeperServer(snapshotDir.toFile(), logDir.toFile(), tickTime);
        factory = new NIOServerCnxnFactory();
        InetSocketAddress addr = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), port);
        LOGGER.info("Starting Zookeeper at " + addr);
        factory.configure(addr, 0);
        factory.startup(zookeeper);
    }

    public void shutdown() throws IOException {
        try {
            zookeeper.shutdown();
        } catch (Exception e) {
            LOGGER.error("Failed to shutdown ZK server", e);
        }

        try {
            factory.shutdown();
        } catch (Exception e) {
            LOGGER.error("Failed to shutdown Zk connection factory.", e);
        }

        FileUtils.deleteDirectory(logDir.toFile());
        FileUtils.deleteDirectory(snapshotDir.toFile());
    }

    public ZooKeeperServer getZk() {
        return zookeeper;
    }
}
