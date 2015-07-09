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

import org.apache.sentry.core.model.sqoop.SqoopActionConstant;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;
import org.apache.sqoop.security.SecurityError;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestServerScopeEndToEnd extends AbstractSqoopSentryTestBase {

  @Test
  public void testServerScopePrivilege() throws Exception {
    /**
     * ADMIN_USER create two links and one job
     */
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MLink rdbmsLink = client.createLink("generic-jdbc-connector");
    sqoopServerRunner.fillRdbmsLinkConfig(rdbmsLink);
    sqoopServerRunner.saveLink(client, rdbmsLink);

    MLink hdfsLink = client.createLink("hdfs-connector");
    sqoopServerRunner.fillHdfsLink(hdfsLink);
    sqoopServerRunner.saveLink(client, hdfsLink);

    MJob job1 = client.createJob(hdfsLink.getPersistenceId(), rdbmsLink.getPersistenceId());
    // set HDFS "FROM" config for the job, since the connector test case base class only has utilities for HDFS!
    sqoopServerRunner.fillHdfsFromConfig(job1);
    // set the RDBM "TO" config here
    sqoopServerRunner.fillRdbmsToConfig(job1);
    // create job
    sqoopServerRunner.saveJob(client, job1);


    MResource  sqoopServer1 = new MResource(SQOOP_SERVER_NAME, MResource.TYPE.SERVER);
    /**
     * ADMIN_USER grant read privilege on server SQOOP_SERVER_NAME to role1
     */
    MRole role1 = new MRole(ROLE1);
    MPrincipal group1 = new MPrincipal(GROUP1, MPrincipal.TYPE.GROUP);
    MPrivilege readPrivilege = new MPrivilege(sqoopServer1,SqoopActionConstant.READ, false);
    client.createRole(role1);
    client.grantRole(Lists.newArrayList(role1), Lists.newArrayList(group1));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role1.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(readPrivilege));

    /**
     * ADMIN_USER grant write privilege on server SQOOP_SERVER_NAME to role2
     * ADMIN_USER grant read privilege on connector all to role2 (for update link required)
     * ADMIN_USER grant read privilege on link all to role2 (for update job required)
     */
    MRole role2 = new MRole(ROLE2);
    MPrincipal group2 = new MPrincipal(GROUP2, MPrincipal.TYPE.GROUP);
    MPrivilege writePrivilege = new MPrivilege(sqoopServer1,SqoopActionConstant.WRITE, false);
    client.createRole(role2);

    MResource allConnector = new MResource(SqoopActionConstant.ALL, MResource.TYPE.CONNECTOR);
    MResource allLink = new MResource(SqoopActionConstant.ALL, MResource.TYPE.LINK);
    MPrivilege readAllConPriv = new MPrivilege(allConnector,SqoopActionConstant.READ, false);
    MPrivilege readAllLinkPriv = new MPrivilege(allLink,SqoopActionConstant.READ, false);

    client.grantRole(Lists.newArrayList(role2), Lists.newArrayList(group2));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role2.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(writePrivilege, readAllConPriv, readAllLinkPriv));

    /**
     * ADMIN_USER grant all privilege on server SQOOP_SERVER_NAME to role3
     */
    MRole role3 = new MRole(ROLE3);
    MPrincipal group3 = new MPrincipal(GROUP3, MPrincipal.TYPE.GROUP);
    MPrivilege allPrivilege = new MPrivilege(sqoopServer1,SqoopActionConstant.ALL_NAME, false);
    client.createRole(role3);
    client.grantRole(Lists.newArrayList(role3), Lists.newArrayList(group3));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role3.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(allPrivilege));

    /**
     * user1 has only the read privilege on server SQOOP_SERVER_NAME to role1,
     * so user1 can show connector, link and jobs. The user1 can't update the link and
     * job
     */
    client = sqoopServerRunner.getSqoopClient(USER1);
    try {
      // show connector
      assertTrue(client.getConnector("generic-jdbc-connector") != null);
      assertTrue(client.getConnector("hdfs-connector") != null);
      assertTrue(client.getConnectors().size() > 0);
      // show link
      assertTrue(client.getLink(hdfsLink.getPersistenceId()) != null);
      assertTrue(client.getLink(rdbmsLink.getPersistenceId()) != null);
      assertTrue(client.getLinks().size() == 2);
      // show job
      assertTrue(client.getJob(job1.getPersistenceId()) != null);
      assertTrue(client.getJobs().size() == 1);
    } catch (Exception e) {
      fail("unexpected Authorization exception happend");
    }
    // user1 can't update link and job
    try {
      hdfsLink.setName("hdfs1_update_user1");
      client.updateLink(hdfsLink);
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }

    try {
      job1.setName("job1_update_user1");
      client.updateJob(job1);
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }

    /**
     * user2 has the write privilege on server SQOOP_SERVER_NAME to role2. In order to update link and job,
     * user2 also has the read privilege on connector all and link all
     * user2 can update link and jobs. The user2 can't show job
     */
    client = sqoopServerRunner.getSqoopClient(USER2);
    try {
      // update link and job
      hdfsLink.setName("hdfs1_update_user2");
      client.updateLink(hdfsLink);
      job1.setName("job1_update_user2");
      client.updateJob(job1);
    } catch (Exception e) {
      fail("unexpected Authorization exception happend");
    }
    // user2 can't show job
    assertTrue(client.getJobs().size() == 0);

    /**
     * user3 has the all privilege on server SQOOP_SERVER_NAME to role3.
     * user3 can do any operation on any sqoop resource
     */
    client = sqoopServerRunner.getSqoopClient(USER3);
    try {
      // show connector
      assertTrue(client.getConnector("generic-jdbc-connector") != null);
      assertTrue(client.getConnector("hdfs-connector") != null);
      assertTrue(client.getConnectors().size() > 0);
      // show link
      assertTrue(client.getLink(hdfsLink.getPersistenceId()) != null);
      assertTrue(client.getLink(rdbmsLink.getPersistenceId()) != null);
      assertTrue(client.getLinks().size() == 2);
      // show job
      assertTrue(client.getJob(job1.getPersistenceId()) != null);
      assertTrue(client.getJobs().size() == 1);
      // update link
      hdfsLink.setName("hdfs1_update_user3");
      client.updateLink(hdfsLink);
      // update job
      job1.setName("job1_update_user3");
      client.updateJob(job1);
    } catch (Exception e) {
      fail("unexpected Authorization exception happend");
    }
  }
}
