/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.hdfs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.curator.test.TestingServer;
import org.apache.sentry.hdfs.service.thrift.TRoleChanges;
import org.apache.sentry.provider.db.service.persistent.HAContext;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestHAUpdateForwarder extends TestUpdateForwarder {

  private TestingServer server;

  @Before
  public void setup() throws Exception {
    server = new TestingServer();
    server.start();
    testConf.set(ServerConfig.SENTRY_HA_ZOOKEEPER_QUORUM,
        server.getConnectString());
    testConf.setBoolean(ServerConfig.SENTRY_HA_ENABLED, true);
  }

  @Override
  @After
  public void cleanup() throws Exception {
    super.cleanup();
    server.stop();
    HAContext.clearServerContext();
  }

  @Test
  public void testThriftSerializer() throws Exception {
    List<String> addGroups = Lists.newArrayList("g1", "g2", "g3");
    List<String> delGroups = Lists.newArrayList("d1", "d2", "d3");
    String roleName = "testRole1";

    TRoleChanges roleUpdate = new TRoleChanges(roleName, addGroups, delGroups);
    TRoleChanges newRoleUpdate = (TRoleChanges) ThriftSerializer.deserialize(
        roleUpdate, ThriftSerializer.serialize(roleUpdate));
    assertEquals(roleUpdate, newRoleUpdate);
  }
}
