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
import static org.junit.Assert.assertEquals;
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

public class TestOwnerPrivilege extends AbstractSqoopSentryTestBase {

  @Test
  public void testLinkOwner() throws Exception {
    // USER1 at firstly has no privilege on any Sqoop resource
    SqoopClient client = sqoopServerRunner.getSqoopClient(USER1);
    assertTrue(client.getConnectors().size() == 0);
    /**
     * ADMIN_USER grant read action privilege on connector HDFS_CONNECTOR_NAME to role ROLE1
     * ADMIN_USER grant role ROLE1 to group GROUP1
     */
    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role1 = new MRole(ROLE1);
    MPrincipal group1 = new MPrincipal(GROUP1, MPrincipal.TYPE.GROUP);
    MResource hdfsConnector = new MResource(HDFS_CONNECTOR_NAME, MResource.TYPE.CONNECTOR);
    MPrivilege readPriv = new MPrivilege(hdfsConnector, SqoopActionConstant.READ, false);
    client.createRole(role1);
    client.grantRole(Lists.newArrayList(role1), Lists.newArrayList(group1));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role1.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(readPriv));

    // check USER1 has the read privilege on HDFS_CONNECTOR_NAME connector
    client = sqoopServerRunner.getSqoopClient(USER1);
    assertEquals(client.getConnectors().size(), 1);

    // USER1 create a new HDFS link
    MLink hdfsLink = client.createLink(HDFS_CONNECTOR_NAME);
    hdfsLink.setName("HDFS_link1");
    sqoopServerRunner.fillHdfsLink(hdfsLink);
    sqoopServerRunner.saveLink(client, hdfsLink);

    // USER1 is the owner of HDFS link, so he can show and update HDFS link
    assertEquals(client.getLink(hdfsLink.getName()).getName(), hdfsLink.getName());

    // USER1 update the name of HDFS link
    String oldLinkName = hdfsLink.getName();
    hdfsLink.setName("HDFS_update1");
    sqoopServerRunner.updateLink(client, hdfsLink, oldLinkName);

    // USER2 has no privilege on HDFS link
    client = sqoopServerRunner.getSqoopClient(USER2);
    assertTrue(client.getLinks().size() == 0);

    //delete the HDFS link
    client = sqoopServerRunner.getSqoopClient(USER1);
    client.deleteLink(hdfsLink.getName());
  }

  @Test
  public void testJobOwner() throws Exception {
    // USER3 at firstly has no privilege on any Sqoop resource
    SqoopClient client = sqoopServerRunner.getSqoopClient(USER3);
    assertTrue(client.getConnectors().size() == 0);
    /**
     * ADMIN_USER grant read action privilege on connector HDFS_CONNECTOR_NAME and JDBC_CONNECTOR_NAME to role ROLE3
     * ADMIN_USER grant role ROLE3 to group GROUP3
     */
    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role3 = new MRole(ROLE3);
    MPrincipal group3 = new MPrincipal(GROUP3, MPrincipal.TYPE.GROUP);
    MResource hdfsConnector = new MResource(HDFS_CONNECTOR_NAME, MResource.TYPE.CONNECTOR);
    MResource jdbcConnector = new MResource(JDBC_CONNECTOR_NAME, MResource.TYPE.CONNECTOR);
    MPrivilege readPriv = new MPrivilege(hdfsConnector, SqoopActionConstant.READ, false);
    MPrivilege readPriv2 = new MPrivilege(jdbcConnector, SqoopActionConstant.READ, false);
    client.createRole(role3);
    client.grantRole(Lists.newArrayList(role3), Lists.newArrayList(group3));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(ROLE3, MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(readPriv, readPriv2));

    // check USER3 has the read privilege on the connectors
    client = sqoopServerRunner.getSqoopClient(USER3);
    assertEquals(client.getConnectors().size(), 2);

    // USER3 create two links: hdfs link and rdbm link
    MLink rdbmsLink = client.createLink(JDBC_CONNECTOR_NAME);
    rdbmsLink.setName("JDBC_link1");
    sqoopServerRunner.fillRdbmsLinkConfig(rdbmsLink);
    sqoopServerRunner.saveLink(client, rdbmsLink);

    MLink hdfsLink = client.createLink(HDFS_CONNECTOR_NAME);
    hdfsLink.setName("HDFS_link1");
    sqoopServerRunner.fillHdfsLink(hdfsLink);
    sqoopServerRunner.saveLink(client, hdfsLink);

    // USER3 is the owner of hdfs and link, so he can show and update hdfs link
    assertTrue(client.getLinks().size() == 2);
    String oldLinkName = hdfsLink.getName();
    hdfsLink.setName("HDFS_update2");
    client.updateLink(hdfsLink, oldLinkName);
    oldLinkName = rdbmsLink.getName();
    rdbmsLink.setName("RDBM_update");
    client.updateLink(rdbmsLink, oldLinkName);

    // USER_3 create a job: transfer date from HDFS to RDBM
    MJob job1 = client.createJob(hdfsLink.getName(), rdbmsLink.getName());
    // set HDFS "FROM" config for the job, since the connector test case base class only has utilities for HDFS!
    sqoopServerRunner.fillHdfsFromConfig(job1);

    // set the RDBM "TO" config here
    sqoopServerRunner.fillRdbmsToConfig(job1);

    // create job
    job1.setName("HDFS_RDBM_job1");
    sqoopServerRunner.saveJob(client, job1);

    /**
     *  USER3 is the owner of job1 , so he can show and delete job1.
     *  USER4 has no privilege on job1
     */
    client = sqoopServerRunner.getSqoopClient(USER4);
    assertTrue(client.getJobs().size() == 0);
    try {
      client.deleteJob(job1.getName());
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }
    client = sqoopServerRunner.getSqoopClient(USER3);
    assertEquals(client.getJob(job1.getName()).getName(), job1.getName());
    client.deleteJob(job1.getName());

    // delete the HDFS and RDBM links
    client.deleteLink(hdfsLink.getName());
    client.deleteLink(rdbmsLink.getName());
  }

}
