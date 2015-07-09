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
package org.apache.sentry.tests.e2e.sqoop;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;

import org.apache.sentry.core.model.sqoop.SqoopActionConstant;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestConnectorEndToEnd extends AbstractSqoopSentryTestBase {
  private static String JDBC_CONNECTOR_NAME = "generic-jdbc-connector";
  private static String HDFS_CONNECTOR_NAME = "hdfs-connector";

  @Test
  public void testShowAllConnector() throws Exception {
    // USER3 at firstly has no privilege on any Sqoop resource
    SqoopClient client = sqoopServerRunner.getSqoopClient(USER3);
    assertTrue(client.getConnectors().size() == 0);
    /**
     * ADMIN_USER grant read action privilege on connector all to role ROLE3
     * ADMIN_USER grant role ROLE3 to group GROUP3
     */
    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role3 = new MRole(ROLE3);
    MPrincipal group3 = new MPrincipal(GROUP3, MPrincipal.TYPE.GROUP);
    MResource  allConnector = new MResource(SqoopActionConstant.ALL, MResource.TYPE.CONNECTOR);
    MPrivilege readPriv = new MPrivilege(allConnector,SqoopActionConstant.READ, false);
    client.createRole(role3);
    client.grantRole(Lists.newArrayList(role3), Lists.newArrayList(group3));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role3.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(readPriv));

    // check USER3 has the read privilege on all connector
    client = sqoopServerRunner.getSqoopClient(USER3);
    assertTrue(client.getConnectors().size() > 0);
  }

  @Test
  public void testShowSpecificConnector() throws Exception {
    // USER1 and USER2 at firstly has no privilege on any Sqoop resource
    SqoopClient client = sqoopServerRunner.getSqoopClient(USER1);
    assertTrue(client.getConnectors().size() == 0);
    client = sqoopServerRunner.getSqoopClient(USER2);
    assertTrue(client.getConnectors().size() == 0);

    /**
     * ADMIN_USER grant read action privilege on jdbc connector to role ROLE1
     * ADMIN_USER grant read action privilege on hdfs connector to role ROLE2
     */
    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MConnector hdfsConnector = client.getConnector(HDFS_CONNECTOR_NAME);
    MConnector jdbcConnector = client.getConnector(JDBC_CONNECTOR_NAME);

    MRole role1 = new MRole(ROLE1);
    MPrincipal group1 = new MPrincipal(GROUP1, MPrincipal.TYPE.GROUP);
    MPrivilege readHdfsPriv = new MPrivilege(new MResource(String.valueOf(hdfsConnector.getPersistenceId()), MResource.TYPE.CONNECTOR),
        SqoopActionConstant.READ, false);
    client.createRole(role1);
    client.grantRole(Lists.newArrayList(role1), Lists.newArrayList(group1));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role1.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(readHdfsPriv));

    MRole role2 = new MRole(ROLE2);
    MPrincipal group2 = new MPrincipal(GROUP2, MPrincipal.TYPE.GROUP);
    MPrivilege readJdbcPriv = new MPrivilege(new MResource(String.valueOf(jdbcConnector.getPersistenceId()), MResource.TYPE.CONNECTOR),
        SqoopActionConstant.READ, false);
    client.createRole(role2);
    client.grantRole(Lists.newArrayList(role2), Lists.newArrayList(group2));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role2.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(readJdbcPriv));

    client = sqoopServerRunner.getSqoopClient(USER1);
    assertTrue(client.getConnectors().size() == 1);
    // user1 can show hdfs connector
    assertTrue(client.getConnector(HDFS_CONNECTOR_NAME) != null);
    // user1 can't show jdbc connector
    assertTrue(client.getConnector(JDBC_CONNECTOR_NAME) == null);

    client = sqoopServerRunner.getSqoopClient(USER2);
    assertTrue(client.getConnectors().size() == 1);
    // user2 can show jdbc connector
    assertTrue(client.getConnector(JDBC_CONNECTOR_NAME) != null);
    // user2 can't show hdfs connector
    assertTrue(client.getConnector(HDFS_CONNECTOR_NAME) == null);
  }
}
