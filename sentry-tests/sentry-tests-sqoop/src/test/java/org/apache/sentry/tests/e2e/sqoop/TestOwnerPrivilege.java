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
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MDriverConfig;
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
     * ADMIN_USER grant read action privilege on connector all to role ROLE1
     * ADMIN_USER grant role ROLE1 to group GROUP1
     */
    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MRole role1 = new MRole(ROLE1);
    MPrincipal group1 = new MPrincipal(GROUP1, MPrincipal.TYPE.GROUP);
    MResource  allConnector = new MResource(SqoopActionConstant.ALL, MResource.TYPE.CONNECTOR);
    MPrivilege readPriv = new MPrivilege(allConnector,SqoopActionConstant.READ, false);
    client.createRole(role1);
    client.grantRole(Lists.newArrayList(role1), Lists.newArrayList(group1));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role1.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(readPriv));

    // check USER1 has the read privilege on all connector
    client = sqoopServerRunner.getSqoopClient(USER1);
    assertTrue(client.getConnectors().size() > 0);

    // USER1 create a new HDFS link
    MLink hdfsLink = client.createLink("hdfs-connector");
    sqoopServerRunner.fillHdfsLink(hdfsLink);
    sqoopServerRunner.saveLink(client, hdfsLink);

    // USER1 is the owner of HDFS link, so he can show and update HDFS link
    assertEquals(client.getLink(hdfsLink.getPersistenceId()), hdfsLink);

    // USER1 update the name of HDFS link
    hdfsLink.setName("HDFS_update1");
    sqoopServerRunner.updateLink(client, hdfsLink);

    // USER2 has no privilege on HDFS link
    client = sqoopServerRunner.getSqoopClient(USER2);
    assertTrue(client.getLinks().size() == 0);

    //delete the HDFS link
    client = sqoopServerRunner.getSqoopClient(USER1);
    client.deleteLink(hdfsLink.getPersistenceId());
  }

  @Test
  public void testJobOwner() throws Exception {
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
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(ROLE3, MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(readPriv));

    // check USER3 has the read privilege on all connector
    client = sqoopServerRunner.getSqoopClient(USER3);
    assertTrue(client.getConnectors().size() > 0);

    // USER3 create two links: hdfs link and rdbm link
    MLink rdbmsLink = client.createLink("generic-jdbc-connector");
    sqoopServerRunner.fillRdbmsLinkConfig(rdbmsLink);
    sqoopServerRunner.saveLink(client, rdbmsLink);

    MLink hdfsLink = client.createLink("hdfs-connector");
    sqoopServerRunner.fillHdfsLink(hdfsLink);
    sqoopServerRunner.saveLink(client, hdfsLink);

    // USER3 is the owner of hdfs and link, so he can show and update hdfs link
    assertTrue(client.getLinks().size() == 2);
    hdfsLink.setName("HDFS_update2");
    client.updateLink(hdfsLink);
    rdbmsLink.setName("RDBM_update");
    client.updateLink(rdbmsLink);

    // USER_3 create a job: transfer date from HDFS to RDBM
    MJob job1 = client.createJob(hdfsLink.getPersistenceId(), rdbmsLink.getPersistenceId());
    // set HDFS "FROM" config for the job, since the connector test case base class only has utilities for HDFS!
    sqoopServerRunner.fillHdfsFromConfig(job1);

    // set the RDBM "TO" config here
    sqoopServerRunner.fillRdbmsToConfig(job1);

    // create job
    sqoopServerRunner.saveJob(client, job1);

    /**
     *  USER3 is the owner of job1 , so he can show and delete job1.
     *  USER4 has no privilege on job1
     */
    client = sqoopServerRunner.getSqoopClient(USER4);
    assertTrue(client.getJobs().size() == 0);
    try {
      client.deleteJob(job1.getPersistenceId());
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }
    client = sqoopServerRunner.getSqoopClient(USER3);
    assertEquals(client.getJob(job1.getPersistenceId()), job1);
    client.deleteJob(job1.getPersistenceId());

    // delete the HDFS and RDBM links
    client.deleteLink(hdfsLink.getPersistenceId());
    client.deleteLink(rdbmsLink.getPersistenceId());
  }

}
