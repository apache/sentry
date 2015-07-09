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

public class TestJobEndToEnd extends AbstractSqoopSentryTestBase {
  @Test
  public void testShowJob() throws Exception {
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
    /**
     * ADMIN_USER grant read privilege on all job to role1
     */
    MRole role1 = new MRole(ROLE1);
    MPrincipal group1 = new MPrincipal(GROUP1, MPrincipal.TYPE.GROUP);
    MResource allJob = new MResource(SqoopActionConstant.ALL, MResource.TYPE.JOB);
    MPrivilege readAllPrivilege = new MPrivilege(allJob,SqoopActionConstant.READ, false);
    client.createRole(role1);
    client.grantRole(Lists.newArrayList(role1), Lists.newArrayList(group1));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role1.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(readAllPrivilege));

    /**
     * ADMIN_USER grant read privilege on job1 to role2
     */
    MRole role2 = new MRole(ROLE2);
    MPrincipal group2 = new MPrincipal(GROUP2, MPrincipal.TYPE.GROUP);
    MResource job1Resource = new MResource(String.valueOf(job1.getPersistenceId()), MResource.TYPE.JOB);
    MPrivilege readJob1Privilege = new MPrivilege(job1Resource,SqoopActionConstant.READ, false);
    client.createRole(role2);
    client.grantRole(Lists.newArrayList(role2), Lists.newArrayList(group2));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role2.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(readJob1Privilege));

    // user1 can show all jobs
    client = sqoopServerRunner.getSqoopClient(USER1);
    try {
      assertTrue(client.getJobs().size() == 1);
      assertTrue(client.getJob(job1.getPersistenceId()) != null);
    } catch (Exception e) {
      fail("unexpected Authorization exception happend");
    }

    // user2 can show job1
    client = sqoopServerRunner.getSqoopClient(USER2);
    try {
      assertTrue(client.getJobs().size() == 1);
      assertTrue(client.getJob(job1.getPersistenceId()) != null);
    } catch (Exception e) {
      fail("unexpected Authorization exception happend");
    }

    // user3 can't show job1
    client = sqoopServerRunner.getSqoopClient(USER3);
    try {
      assertTrue(client.getJobs().size() == 0);
      client.getJob(job1.getPersistenceId());
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }

    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    client.deleteJob(job1.getPersistenceId());
  }

  @Test
  public void testUpdateDeleteJob() throws Exception {
    /**
     * ADMIN_USER create two links and one job
     */
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MLink rdbmsLink = client.createLink("generic-jdbc-connector");
    sqoopServerRunner.fillRdbmsLinkConfig(rdbmsLink);
    rdbmsLink.setName("rdbm_testUpdateJob");
    sqoopServerRunner.saveLink(client, rdbmsLink);

    MLink hdfsLink = client.createLink("hdfs-connector");
    sqoopServerRunner.fillHdfsLink(hdfsLink);
    hdfsLink.setName("hdfs_testUpdateJob");
    sqoopServerRunner.saveLink(client, hdfsLink);

    MJob job2 = client.createJob(hdfsLink.getPersistenceId(), rdbmsLink.getPersistenceId());
    // set HDFS "FROM" config for the job, since the connector test case base class only has utilities for HDFS!
    sqoopServerRunner.fillHdfsFromConfig(job2);
    // set the RDBM "TO" config here
    sqoopServerRunner.fillRdbmsToConfig(job2);
    // create job
    sqoopServerRunner.saveJob(client, job2);

    /**
     * ADMIN_USER grant update privilege on job2 to role4
     * ADMIN_USER grant read privilege on all connector to role4
     * ADMIN_USER grant read privilege on all link to role4
     */
    MRole role4 = new MRole(ROLE4);
    MPrincipal group4 = new MPrincipal(GROUP4, MPrincipal.TYPE.GROUP);
    MResource job2Resource = new MResource(String.valueOf(job2.getPersistenceId()), MResource.TYPE.JOB);
    MPrivilege writeJob2Privilege = new MPrivilege(job2Resource,SqoopActionConstant.WRITE, false);
    MResource  allConnector = new MResource(SqoopActionConstant.ALL, MResource.TYPE.CONNECTOR);
    MPrivilege readConnectorPriv = new MPrivilege(allConnector,SqoopActionConstant.READ, false);
    MResource  allLink = new MResource(SqoopActionConstant.ALL, MResource.TYPE.LINK);
    MPrivilege readLinkPriv = new MPrivilege(allLink,SqoopActionConstant.READ, false);
    client.createRole(role4);
    client.grantRole(Lists.newArrayList(role4), Lists.newArrayList(group4));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role4.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(writeJob2Privilege, readConnectorPriv, readLinkPriv));

    // user4 can't show job2
    client = sqoopServerRunner.getSqoopClient(USER4);
    try {
      assertTrue(client.getJobs().size() == 0);
      client.getJob(job2.getPersistenceId());
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }
    // user4 can update job2
    try {
      job2.setName("job2_update_user4_1");
      client.updateJob(job2);
    } catch (Exception e) {
      fail("unexpected Authorization exception happend");
    }
    // user3 can't update job2
    client = sqoopServerRunner.getSqoopClient(USER3);
    try {
      assertTrue(client.getJobs().size() == 0);
      job2.setName("job2_update_user3_1");
      client.updateJob(job2);
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }

    // user3 can't delete job2
    try {
      client.deleteJob(job2.getPersistenceId());
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }

    //user4 can delete job2 because user4 has write privilege on job2
    client = sqoopServerRunner.getSqoopClient(USER4);
    try {
      client.deleteJob(job2.getPersistenceId());
    } catch (Exception e) {
      fail("unexpected Authorization exception happend");
    }

    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    client.dropRole(role4);
  }

  @Test
  public void testEnableAndStartJob() throws Exception {
    /**
     * ADMIN_USER create two links and one job
     */
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MLink rdbmsLink = client.createLink("generic-jdbc-connector");
    sqoopServerRunner.fillRdbmsLinkConfig(rdbmsLink);
    rdbmsLink.setName("rdbm_testEnableAndStartJob");
    sqoopServerRunner.saveLink(client, rdbmsLink);

    MLink hdfsLink = client.createLink("hdfs-connector");
    sqoopServerRunner.fillHdfsLink(hdfsLink);
    hdfsLink.setName("hdfs_testEnableAndStartJob");
    sqoopServerRunner.saveLink(client, hdfsLink);

    MJob job2 = client.createJob(hdfsLink.getPersistenceId(), rdbmsLink.getPersistenceId());
    // set HDFS "FROM" config for the job, since the connector test case base class only has utilities for HDFS!
    sqoopServerRunner.fillHdfsFromConfig(job2);
    // set the RDBM "TO" config here
    sqoopServerRunner.fillRdbmsToConfig(job2);
    // create job
    sqoopServerRunner.saveJob(client, job2);

    /**
     * ADMIN_USER grant update privilege on job2 to role4
     * ADMIN_USER grant read privilege on all connector to role4
     * ADMIN_USER grant read privilege on all link to role4
     */
    MRole role4 = new MRole(ROLE4);
    MPrincipal group4 = new MPrincipal(GROUP4, MPrincipal.TYPE.GROUP);
    MResource job2Resource = new MResource(String.valueOf(job2.getPersistenceId()), MResource.TYPE.JOB);
    MPrivilege writeJob2Privilege = new MPrivilege(job2Resource,SqoopActionConstant.WRITE, false);
    MResource  allConnector = new MResource(SqoopActionConstant.ALL, MResource.TYPE.CONNECTOR);
    MPrivilege readConnectorPriv = new MPrivilege(allConnector,SqoopActionConstant.READ, false);
    MResource  allLink = new MResource(SqoopActionConstant.ALL, MResource.TYPE.LINK);
    MPrivilege readLinkPriv = new MPrivilege(allLink,SqoopActionConstant.READ, false);
    client.createRole(role4);
    client.grantRole(Lists.newArrayList(role4), Lists.newArrayList(group4));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role4.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(writeJob2Privilege, readConnectorPriv, readLinkPriv));


    /**
     * ADMIN_USER grant read privilege on job2 to role5
     * ADMIN_USER grant read privilege on all connector to role5
     * ADMIN_USER grant read privilege on all link to role5
     */
    MRole role5 = new MRole(ROLE5);
    MPrincipal group5 = new MPrincipal(GROUP5, MPrincipal.TYPE.GROUP);
    MPrivilege readJob2Privilege = new MPrivilege(job2Resource,SqoopActionConstant.READ, false);
    client.createRole(role5);
    client.grantRole(Lists.newArrayList(role5), Lists.newArrayList(group5));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role5.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(readJob2Privilege, readConnectorPriv, readLinkPriv));

    // user5 can't enable and start job2
    client = sqoopServerRunner.getSqoopClient(USER5);
    try {
      client.enableJob(job2.getPersistenceId(), true);
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }

    try {
      client.startJob(job2.getPersistenceId());
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }

    // user3 can't enable and start job2
    client = sqoopServerRunner.getSqoopClient(USER3);
    try {
      client.enableJob(job2.getPersistenceId(), true);
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }

    try {
      client.startJob(job2.getPersistenceId());
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }

    // user4 can enable or start job2
    client = sqoopServerRunner.getSqoopClient(USER4);
    try {
      client.enableJob(job2.getPersistenceId(), false);
      client.enableJob(job2.getPersistenceId(), true);
      client.deleteJob(job2.getPersistenceId());
    } catch (Exception e) {
      fail("unexpected Authorization exception happend");
    }


    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    client.dropRole(role4);
    client.dropRole(role5);
  }
}
