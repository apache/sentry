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

import kafka.server.KafkaServerStartable;
import org.apache.sentry.kafka.conf.KafkaAuthConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class KafkaTestServer {
    public static final int CACHE_TTL_MS = 1;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestServer.class);

    private int zkPort = -1;
    private int kafkaPort = -1;
    private EmbeddedZkServer zkServer = null;
    private KafkaServerStartable kafkaServer = null;
    private File sentrySitePath = null;

    public KafkaTestServer(File sentrySitePath) throws Exception {
        this.sentrySitePath = sentrySitePath;
        this.zkPort = TestUtils.getFreePort();
        createZkServer();
        this.kafkaPort = TestUtils.getFreePort();
        createKafkaServer();
    }

    public void start() throws Exception {
        kafkaServer.startup();
        LOGGER.info("Started Kafka broker.");
    }

    public void shutdown() {
        if (kafkaServer != null) {
            kafkaServer.shutdown();
            kafkaServer.awaitShutdown();
            LOGGER.info("Stopped Kafka server.");
        }

        if (zkServer != null) {
            try {
                zkServer.shutdown();
                LOGGER.info("Stopped ZK server.");
            } catch (IOException e) {
                LOGGER.error("Failed to shutdown ZK server.", e);
            }
        }
    }

    private Path getTempDirectory() {
        Path tempDirectory = null;
        try {
            tempDirectory = Files.createTempDirectory("kafka-sentry-");
        } catch (IOException e) {
            LOGGER.error("Failed to create temp dir for Kafka's log dir.");
            throw new RuntimeException(e);
        }
        return tempDirectory;
    }

    private void setupKafkaProps(Properties props) throws UnknownHostException {
        props.put("listeners", "SSL://" + InetAddress.getLocalHost().getHostAddress() + ":" + kafkaPort);
        props.put("log.dir", getTempDirectory().toAbsolutePath().toString());
        props.put("zookeeper.connect", InetAddress.getLocalHost().getHostAddress() + ":" + zkPort);
        props.put("replica.socket.timeout.ms", "1500");
        props.put("controller.socket.timeout.ms", "1500");
        props.put("controlled.shutdown.enable", true);
        props.put("delete.topic.enable", false);
        props.put("controlled.shutdown.retry.backoff.ms", "100");
        props.put("port", kafkaPort);
        props.put("authorizer.class.name", "org.apache.sentry.kafka.authorizer.SentryKafkaAuthorizer");
        props.put("sentry.kafka.site.url", "file://" + sentrySitePath.getAbsolutePath());
        props.put("allow.everyone.if.no.acl.found", "true");
        props.put("ssl.keystore.location", KafkaTestServer.class.getResource("/test.keystore.jks").getPath());
        props.put("ssl.keystore.password", "test-ks-passwd");
        props.put("ssl.key.password", "test-key-passwd");
        props.put("ssl.truststore.location", KafkaTestServer.class.getResource("/test.truststore.jks").getPath());
        props.put("ssl.truststore.password", "test-ts-passwd");
        props.put("security.inter.broker.protocol", "SSL");
        props.put("ssl.client.auth", "required");
        props.put(KafkaAuthConf.KAFKA_SUPER_USERS, "User:CN=superuser;User:CN=superuser1; User:CN=Superuser2 ");
        props.put(KafkaAuthConf.SENTRY_KAFKA_CACHING_ENABLE_NAME, "true");
        props.put(KafkaAuthConf.SENTRY_KAFKA_CACHING_TTL_MS_NAME, String.valueOf(CACHE_TTL_MS));
    }

    private void createKafkaServer() throws UnknownHostException {
        Properties props = new Properties();
        setupKafkaProps(props);
        kafkaServer = KafkaServerStartable.fromProps(props);
    }

    private void createZkServer() throws Exception {
        try {
            zkServer = new EmbeddedZkServer(zkPort);
            zkPort = zkServer.getZk().getClientPort();
        } catch (Exception e) {
            LOGGER.error("Failed to create testing zookeeper server.");
            throw new RuntimeException(e);
        }
    }

    public String getBootstrapServers() throws UnknownHostException {
        return InetAddress.getLocalHost().getHostAddress() + ":" + kafkaPort;
    }
}
