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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.sentry.core.model.sqoop.SqoopActionConstant;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;
import org.apache.sqoop.security.SecurityError;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestLinkEndToEnd extends AbstractSqoopSentryTestBase {

  private void dropAndCreateRole(SqoopClient client, MRole mrole) throws Exception {
    try {
      client.dropRole(mrole);
    } catch (Exception e) {
      // nothing to do if role doesn't exist
    }
    client.createRole(mrole);
  }

  @Test
  public void testShowLink() throws Exception {
    /**
     * ADMIN_USER create a hdfs link
     */
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MLink hdfsLink = client.createLink(HDFS_CONNECTOR_NAME);
    sqoopServerRunner.fillHdfsLink(hdfsLink);
    hdfsLink.setName("hdfs-link1");
    sqoopServerRunner.saveLink(client, hdfsLink);

    /**
     * ADMIN_USER grant read privilege on "hdfs-link1" link to role1
     */
    MRole role1 = new MRole(ROLE1);
    MPrincipal group1 = new MPrincipal(GROUP1, MPrincipal.TYPE.GROUP);
    MResource hdfsLinkResource = new MResource("hdfs-link1", MResource.TYPE.LINK);
    MPrivilege readHdfsLinkPrivilege = new MPrivilege(hdfsLinkResource, SqoopActionConstant.READ, false);
    dropAndCreateRole(client, role1);
    client.grantRole(Lists.newArrayList(role1), Lists.newArrayList(group1));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role1.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(readHdfsLinkPrivilege));

    /**
     * ADMIN_USER grant read privilege on hdfs link to role2
     */
    MRole role2 = new MRole(ROLE2);
    MPrincipal group2 = new MPrincipal(GROUP2, MPrincipal.TYPE.GROUP);
    dropAndCreateRole(client, role2);
    client.grantRole(Lists.newArrayList(role2), Lists.newArrayList(group2));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role2.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(readHdfsLinkPrivilege));

    // user1 can show all link
    client = sqoopServerRunner.getSqoopClient(USER1);
    try {
      assertTrue(client.getLinks().size() == 1);
      assertTrue(client.getLink(hdfsLink.getName()) != null);
    } catch (Exception e) {
      fail("unexpected Authorization exception happend");
    }

    // user2 can show hdfs link
    client = sqoopServerRunner.getSqoopClient(USER2);
    try {
      assertTrue(client.getLinks().size() == 1);
      assertTrue(client.getLink(hdfsLink.getName()) != null);
    } catch (Exception e) {
      fail("unexpected Authorization exception happend");
    }

    // user3 can't show hdfs link
    client = sqoopServerRunner.getSqoopClient(USER3);
    assertTrue(client.getLinks().size() == 0);
    try {
        assertNull(client.getLink(hdfsLink.getName()));
    } catch (IndexOutOfBoundsException ex) {
        // This try statement can be removed when https://issues.apache.org/jira/browse/SQOOP-3250 is fixed
    }

    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    client.deleteLink(hdfsLink.getName());
  }

  @Test
  public void testUpdateDtestUpdateDeleteLinkeleteLink() throws Exception {
    /**
     * ADMIN_USER create a hdfs link
     */
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MLink hdfsLink = client.createLink(HDFS_CONNECTOR_NAME);
    sqoopServerRunner.fillHdfsLink(hdfsLink);
    hdfsLink.setName("hdfs-link1");
    sqoopServerRunner.saveLink(client, hdfsLink);

    /**
     * ADMIN_USER grant update privilege on hdfs link to role4
     * ADMIN_USER grant read privilege on HDFS_CONNECTOR_NAME connector to role4
     */
    MRole role4 = new MRole(ROLE4);
    MPrincipal group4 = new MPrincipal(GROUP4, MPrincipal.TYPE.GROUP);
    MResource hdfsLinkResource = new MResource(String.valueOf(hdfsLink.getName()), MResource.TYPE.LINK);
    MPrivilege writeHdfsPrivilege = new MPrivilege(hdfsLinkResource,SqoopActionConstant.WRITE, false);
    MResource hdfsConnector = new MResource(HDFS_CONNECTOR_NAME, MResource.TYPE.CONNECTOR);
    MPrivilege readConnectorPriv = new MPrivilege(hdfsConnector, SqoopActionConstant.READ, false);
    dropAndCreateRole(client, role4);
    client.grantRole(Lists.newArrayList(role4), Lists.newArrayList(group4));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role4.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(writeHdfsPrivilege, readConnectorPriv));

    // user4 can't show hdfs link

    client = sqoopServerRunner.getSqoopClient(USER4);
    assertTrue(client.getLinks().size() == 0);
    try {
        assertNull(client.getLink(hdfsLink.getName()));
    } catch (IndexOutOfBoundsException ex) {
        // This try statement can be removed when https://issues.apache.org/jira/browse/SQOOP-3250 is fixed
    }

    // user4 can update hdfs link
    try {
      hdfsLink.setName("hdfs_link_update_user4_1");
      client.updateLink(hdfsLink, "hdfs-link1");

      MResource updatedHdfsLinkResource = new MResource("hdfs_link_update_user4_1", MResource.TYPE.LINK);
      MPrivilege updatedWriteHdfsPrivilege = new MPrivilege(updatedHdfsLinkResource, SqoopActionConstant.WRITE, false);
      SqoopClient adminClient = sqoopServerRunner.getSqoopClient(ADMIN_USER);
      adminClient.grantPrivilege(Lists.newArrayList(new MPrincipal(role4.getName(), MPrincipal.TYPE.ROLE)),
                            Lists.newArrayList(updatedWriteHdfsPrivilege));

      // Now update it back
      hdfsLink.setName("hdfs-link1");
      client.updateLink(hdfsLink, "hdfs_link_update_user4_1");
    } catch (Exception e) {
      fail("unexpected Authorization exception happend");
    }

    // user3 can't update hdfs link
    client = sqoopServerRunner.getSqoopClient(USER3);
    try {
      assertTrue(client.getLinks().size() == 0);
      hdfsLink.setName("hdfs_link_update_user3_1");
      client.updateLink(hdfsLink, "hdfs-link1");
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
      hdfsLink.setName("hdfs-link1");
    }

    // user3 can't delete hdfs link
    try {
      client.deleteLink(hdfsLink.getName());
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }

    //user4 can delete hdfs link because user4 has write privilege on hdfs link
    client = sqoopServerRunner.getSqoopClient(USER4);
    try {
      client.deleteLink(hdfsLink.getName());
    } catch (Exception e) {
      fail("unexpected Authorization exception happend");
    }

    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    client.dropRole(role4);
  }

  @Test
  public void testEnableLink() throws Exception {
    /**
     * ADMIN_USER create a hdfs link
     */
    SqoopClient client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    MLink hdfsLink = client.createLink(HDFS_CONNECTOR_NAME);
    sqoopServerRunner.fillHdfsLink(hdfsLink);
    hdfsLink.setName("hdfs-link1");
    sqoopServerRunner.saveLink(client, hdfsLink);

    /**
     * ADMIN_USER grant read privilege on hdfs link to role4
     * ADMIN_USER grant read privilege on HDFS_CONNECTOR_NAME connector to role4
     */
    MRole role4 = new MRole(ROLE4);
    MPrincipal group4 = new MPrincipal(GROUP4, MPrincipal.TYPE.GROUP);
    MResource hdfsLinkResource = new MResource(String.valueOf(hdfsLink.getName()), MResource.TYPE.LINK);
    MPrivilege readHdfsPrivilege = new MPrivilege(hdfsLinkResource,SqoopActionConstant.READ, false);
    MResource hdfsConnector = new MResource(HDFS_CONNECTOR_NAME, MResource.TYPE.CONNECTOR);
    MPrivilege readConnectorPriv = new MPrivilege(hdfsConnector, SqoopActionConstant.READ, false);
    dropAndCreateRole(client, role4);
    client.grantRole(Lists.newArrayList(role4), Lists.newArrayList(group4));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role4.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(readHdfsPrivilege, readConnectorPriv));

    /**
     * ADMIN_USER grant write privilege on hdfs link to role5
     * ADMIN_USER grant read privilege on HDFS_CONNECTOR_NAME connector to role5
     */
    MRole role5 = new MRole(ROLE5);
    MPrincipal group5 = new MPrincipal(GROUP5, MPrincipal.TYPE.GROUP);
    MPrivilege writeHdfsPrivilege = new MPrivilege(hdfsLinkResource, SqoopActionConstant.WRITE, false);
    dropAndCreateRole(client, role5);
    client.grantRole(Lists.newArrayList(role5), Lists.newArrayList(group5));
    client.grantPrivilege(Lists.newArrayList(new MPrincipal(role5.getName(), MPrincipal.TYPE.ROLE)),
        Lists.newArrayList(writeHdfsPrivilege, readConnectorPriv));

    // user4 can't enable hdfs link
    client = sqoopServerRunner.getSqoopClient(USER4);
    try {
      client.enableLink(hdfsLink.getName(), true);
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }
    // user5 can enable hdfs link
    client = sqoopServerRunner.getSqoopClient(USER5);
    try {
      client.enableLink(hdfsLink.getName(), true);
    } catch (Exception e) {
      fail("unexpected Authorization exception happend");
    }
    // user3 can't update hdfs link
    client = sqoopServerRunner.getSqoopClient(USER3);
    try {
      client.enableLink(hdfsLink.getName(), true);
      fail("expected Authorization exception happend");
    } catch (Exception e) {
      assertCausedMessage(e, SecurityError.AUTH_0014.getMessage());
    }

    client = sqoopServerRunner.getSqoopClient(ADMIN_USER);
    try {
      client.dropRole(role4);
      client.dropRole(role5);
    } catch (Exception e) {
      // nothing to do if cleanup fails
    }
    client.deleteLink(hdfsLink.getName());
  }
}
