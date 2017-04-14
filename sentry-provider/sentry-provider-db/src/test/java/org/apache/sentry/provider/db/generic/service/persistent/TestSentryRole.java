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
package org.apache.sentry.provider.db.generic.service.persistent;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.File;
import java.util.Arrays;
import java.util.Properties;
import java.util.List;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.PersistenceManagerFactory;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.apache.commons.io.FileUtils;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.provider.db.service.model.MSentryGMPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.apache.sentry.provider.db.service.model.MSentryRole;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
/**
 * The class tests that the new feature SENTRY-398 generic model adds the new field in the MSentryRole
 * will not affect the functionality of the origin hive/impala authorization model
 * Some Tests below make sure that privileges are removed from sentry storage the moment they are not associated to any role.
 * This avoid the need for PrivCleaner to perform periodic cleanup.
 */
public class TestSentryRole {
  private static PersistenceManagerFactory pmf;
  private static File dataDir;

  @Before
  public void setup() throws Exception {
    dataDir = new File(Files.createTempDir(), "sentry_policy_db");
    Properties prop = new Properties();
    prop.setProperty(ServerConfig.JAVAX_JDO_URL, "jdbc:derby:;databaseName=" + dataDir.getPath() + ";create=true");
    prop.setProperty(ServerConfig.JAVAX_JDO_USER, "Sentry");
    prop.setProperty(ServerConfig.JAVAX_JDO_PASS, "Sentry");
    prop.setProperty(ServerConfig.JAVAX_JDO_DRIVER_NAME, "org.apache.derby.jdbc.EmbeddedDriver");
    prop.setProperty("datanucleus.autoCreateSchema", "true");
    prop.setProperty("datanucleus.fixedDatastore", "false");
    prop.setProperty("datanucleus.NontransactionalRead", "false");
    prop.setProperty("datanucleus.NontransactionalWrite", "false");
    pmf = JDOHelper.getPersistenceManagerFactory(prop);
  }

  @After
  public void tearDown() throws Exception {
    pmf.close();
    FileUtils.deleteQuietly(dataDir);
  }

  @Test
  public void grantMixedPrivilegeTest() throws Exception {
    String roleName = "r1";
    //hive/impala privilege
    MSentryPrivilege hivePrivilege = new MSentryPrivilege();
    hivePrivilege.setServerName("hive.server1");
    hivePrivilege.setDbName("db1");
    hivePrivilege.setTableName("tb1");
    hivePrivilege.setPrivilegeScope("table");
    hivePrivilege.setAction("select");
    hivePrivilege.setGrantOption(true);
    //solr privilege
    MSentryGMPrivilege solrPrivilege = new MSentryGMPrivilege();
    solrPrivilege.setComponentName("solr");
    solrPrivilege.setServiceName("solr.server1");
    solrPrivilege.setAuthorizables(Arrays.asList(new Collection("c1")));
    solrPrivilege.setAction("query");
    solrPrivilege.setGrantOption(true);

    PersistenceManager pm = null;
    //create role
    pm = openTransaction();
    pm.makePersistent(new MSentryRole(roleName, System.currentTimeMillis()));
    commitTransaction(pm);
    //add hivePrivilege to role
    pm = openTransaction();
    MSentryRole role = getMSentryRole(pm, roleName);
    hivePrivilege.appendRole(role);
    pm.makePersistent(hivePrivilege);
    commitTransaction(pm);
    //check hivePrivlege and solrPrivilege
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    pm.retrieve(role);
    assertEquals(1, role.getPrivileges().size());
    assertEquals(0, role.getGmPrivileges().size());
    commitTransaction(pm);
    //add solrPrivilege to role
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    pm.retrieve(role);
    solrPrivilege.appendRole(role);
    pm.makePersistent(solrPrivilege);
    commitTransaction(pm);
    //check hivePrivlege and solrPrivilege
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    pm.retrieve(role);
    assertEquals(1, role.getPrivileges().size());
    assertEquals(1, role.getGmPrivileges().size());
    commitTransaction(pm);
  }

  @Test
  public void testWantGrantPrivilegeTwice() throws Exception {
    String roleName = "r1";
    //hive/impala privilege
    MSentryPrivilege hivePrivilege = new MSentryPrivilege();
    hivePrivilege.setServerName("hive.server1");
    hivePrivilege.setDbName("db1");
    hivePrivilege.setTableName("tb1");
    hivePrivilege.setPrivilegeScope("table");
    hivePrivilege.setAction("select");
    hivePrivilege.setURI(SentryStore.NULL_COL);
    hivePrivilege.setColumnName(SentryStore.NULL_COL);
    hivePrivilege.setGrantOption(true);
    //The same hivePrivilege
    MSentryPrivilege hivePrivilege2 = new MSentryPrivilege(hivePrivilege);
    //solr privilege
    MSentryGMPrivilege solrPrivilege = new MSentryGMPrivilege();
    solrPrivilege.setComponentName("solr");
    solrPrivilege.setServiceName("solr.server1");
    solrPrivilege.setAuthorizables(Arrays.asList(new Collection("c1")));
    solrPrivilege.setAction("query");
    solrPrivilege.setGrantOption(true);
    //The same solrPrivilege
    MSentryGMPrivilege solrPrivilege2 = new MSentryGMPrivilege(solrPrivilege);

    PersistenceManager pm = null;
    //create role
    pm = openTransaction();
    pm.makePersistent(new MSentryRole(roleName, System.currentTimeMillis()));
    commitTransaction(pm);

    //grant hivePrivilege and solrPrivilege to role
    pm = openTransaction();
    MSentryRole role = getMSentryRole(pm, roleName);
    solrPrivilege.appendRole(role);
    hivePrivilege.appendRole(role);
    pm.makePersistent(solrPrivilege);
    pm.makePersistent(hivePrivilege);
    commitTransaction(pm);
    //check
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    pm.retrieve(role);
    assertEquals(1, role.getPrivileges().size());
    assertEquals(1, role.getGmPrivileges().size());
    commitTransaction(pm);

    //want to grant the same hivePrivilege and solrPrivilege to role again
    //hivePrivilege2 is equal to hivePrivilege
    //solrPrivilege2 is equal to solrPrivilege
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    pm.retrieve(role);
    if (!role.getGmPrivileges().contains(solrPrivilege2)) {
      fail("unexpect happend: the MSentryGMPrivilege:" + solrPrivilege2 + " already be granted");
    }
    if (!role.getPrivileges().contains(hivePrivilege2)) {
      fail("unexpect happend: the MSentryPrivilege:" + hivePrivilege2 + " already be granted");
    }
    commitTransaction(pm);
  }

  @Test
  public void testMixedRevokePrivilege() throws Exception {
    String roleName = "r1";
    //hive/impala privilege
    MSentryPrivilege hivePrivilege = new MSentryPrivilege();
    hivePrivilege.setServerName("hive.server1");
    hivePrivilege.setDbName("db1");
    hivePrivilege.setTableName("tb1");
    hivePrivilege.setPrivilegeScope("table");
    hivePrivilege.setAction("select");
    hivePrivilege.setURI(SentryStore.NULL_COL);
    hivePrivilege.setColumnName(SentryStore.NULL_COL);
    hivePrivilege.setGrantOption(true);

    //solr privilege
    MSentryGMPrivilege solrPrivilege = new MSentryGMPrivilege();
    solrPrivilege.setComponentName("solr");
    solrPrivilege.setServiceName("solr.server1");
    solrPrivilege.setAuthorizables(Arrays.asList(new Collection("c1")));
    solrPrivilege.setAction("query");
    solrPrivilege.setGrantOption(true);

    PersistenceManager pm = null;
    //create role
    pm = openTransaction();
    pm.makePersistent(new MSentryRole(roleName, System.currentTimeMillis()));
    commitTransaction(pm);

    //grant hivePrivilege and solrPrivilege to role
    pm = openTransaction();
    MSentryRole role = getMSentryRole(pm, roleName);
    hivePrivilege.appendRole(role);
    solrPrivilege.appendRole(role);
    pm.makePersistent(hivePrivilege);
    pm.makePersistent(solrPrivilege);
    commitTransaction(pm);

    //check
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    pm.retrieve(role);
    assertEquals(1, role.getPrivileges().size());
    assertEquals(1, role.getGmPrivileges().size());
    commitTransaction(pm);

    //revoke solrPrivilege from role
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    solrPrivilege = (MSentryGMPrivilege)role.getGmPrivileges().toArray()[0];
    solrPrivilege.removeRole(role);
    pm.makePersistent(solrPrivilege);
    commitTransaction(pm);

    //check
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    pm.retrieve(role);
    assertEquals(1, role.getPrivileges().size());
    assertEquals(0, role.getGmPrivileges().size());
    commitTransaction(pm);

    //revoke hivePrivilege from role
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    pm.retrieve(role);
    hivePrivilege = (MSentryPrivilege)role.getPrivileges().toArray()[0];
    hivePrivilege.removeRole(role);
    pm.makePersistent(hivePrivilege);
    commitTransaction(pm);

    //check
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    pm.retrieve(role);
    assertEquals(0, role.getPrivileges().size());
    assertEquals(0, role.getGmPrivileges().size());
    commitTransaction(pm);
  }

  @Test
  public void testDeletePrivilegeAndRole() throws Exception {
    String roleName = "r1";
    //hive/impala privilege
    MSentryPrivilege hivePrivilege = new MSentryPrivilege();
    hivePrivilege.setServerName("hive.server1");
    hivePrivilege.setDbName("db1");
    hivePrivilege.setTableName("tb1");
    hivePrivilege.setPrivilegeScope("table");
    hivePrivilege.setAction("select");
    hivePrivilege.setURI(SentryStore.NULL_COL);
    hivePrivilege.setColumnName(SentryStore.NULL_COL);
    hivePrivilege.setGrantOption(true);

    //solr privilege
    MSentryGMPrivilege solrPrivilege = new MSentryGMPrivilege();
    solrPrivilege.setComponentName("solr");
    solrPrivilege.setServiceName("solr.server1");
    solrPrivilege.setAuthorizables(Arrays.asList(new Collection("c1")));
    solrPrivilege.setAction("query");
    solrPrivilege.setGrantOption(true);

    PersistenceManager pm = null;
    //create role
    pm = openTransaction();
    pm.makePersistent(new MSentryRole(roleName, System.currentTimeMillis()));
    commitTransaction(pm);

    //grant hivePrivilege and solrPrivilege to role
    pm = openTransaction();
    MSentryRole role = getMSentryRole(pm, roleName);
    hivePrivilege.appendRole(role);
    solrPrivilege.appendRole(role);
    pm.makePersistent(hivePrivilege);
    pm.makePersistent(solrPrivilege);
    commitTransaction(pm);

    //check
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    pm.retrieve(role);
    assertEquals(1, role.getPrivileges().size());
    assertEquals(1, role.getGmPrivileges().size());
    commitTransaction(pm);

    //remove all privileges
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    role.removeGMPrivileges();
    role.removePrivileges();
    pm.makePersistent(role);
    commitTransaction(pm);

    //check
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    pm.retrieve(role);
    assertEquals(0, role.getPrivileges().size());
    assertEquals(0, role.getGmPrivileges().size());
    commitTransaction(pm);

    //delete role
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    pm.deletePersistent(role);
    commitTransaction(pm);

    //check
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    assertTrue(role == null);
    commitTransaction(pm);
  }

  /**
   * Removes a role and makes sure that privileges are removed from sentry storage
   * moment they are not associated to any role.
   * @throws Exception
   */
  @Test
  public void testDeleteRole() throws Exception {
    String roleName = "r1";
    //hive/impala privilege
    MSentryPrivilege hivePrivilege = new MSentryPrivilege();
    hivePrivilege.setServerName("hive.server1");
    hivePrivilege.setDbName("db1");
    hivePrivilege.setTableName("tb1");
    hivePrivilege.setPrivilegeScope("table");
    hivePrivilege.setAction("select");
    hivePrivilege.setURI(SentryStore.NULL_COL);
    hivePrivilege.setColumnName(SentryStore.NULL_COL);
    hivePrivilege.setGrantOption(true);

    //solr privilege
    MSentryGMPrivilege solrPrivilege = new MSentryGMPrivilege();
    solrPrivilege.setComponentName("solr");
    solrPrivilege.setServiceName("solr.server1");
    solrPrivilege.setAuthorizables(Arrays.asList(new Collection("c1")));
    solrPrivilege.setAction("query");
    solrPrivilege.setGrantOption(true);

    PersistenceManager pm = null;
    //create role
    pm = openTransaction();
    pm.makePersistent(new MSentryRole(roleName, System.currentTimeMillis()));
    commitTransaction(pm);

    //grant hivePrivilege and solrPrivilege to role
    pm = openTransaction();
    MSentryRole role = getMSentryRole(pm, roleName);
    hivePrivilege.appendRole(role);
    solrPrivilege.appendRole(role);
    pm.makePersistent(hivePrivilege);
    pm.makePersistent(solrPrivilege);
    pm.makePersistent(role);
    commitTransaction(pm);

    //check
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    pm.retrieve(role);
    assertEquals(1, role.getPrivileges().size());
    assertEquals(1, role.getGmPrivileges().size());
    commitTransaction(pm);

    //delete role
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);

    //  pm.deletePersistent(role);
    role.removePrivileges();
    role.removeGMPrivileges();
    pm.deletePersistent(role);
    commitTransaction(pm);

    //check for privileges
    //There shouldn't be any privilages
    pm = openTransaction();
    Query query = pm.newQuery(MSentryPrivilege.class);
    List<MSentryPrivilege> results = (List<MSentryPrivilege>) query.execute();
    assertEquals(1, results.size());
    Query query1 = pm.newQuery(MSentryGMPrivilege.class);
    List<MSentryGMPrivilege> results1 = (List<MSentryGMPrivilege>) query1.execute();
    assertEquals(1, results1.size());
    commitTransaction(pm);

    //check
    pm = openTransaction();
    role = getMSentryRole(pm, roleName);
    assertTrue(role == null);
    commitTransaction(pm);
  }

  /**
   * Removes a role and makes sure that privileges are not removed from sentry storage if
   * they are associated to any other role as well.
   * @throws Exception
   */
  @Test
  public void testDeleteRole1() throws Exception {
    String roleName1 = "r1";
    String roleName2 = "r2";
    //hive/impala privilege
    MSentryPrivilege hivePrivilege = new MSentryPrivilege();
    hivePrivilege.setServerName("hive.server1");
    hivePrivilege.setDbName("db1");
    hivePrivilege.setTableName("tb1");
    hivePrivilege.setPrivilegeScope("table");
    hivePrivilege.setAction("select");
    hivePrivilege.setURI(SentryStore.NULL_COL);
    hivePrivilege.setColumnName(SentryStore.NULL_COL);
    hivePrivilege.setGrantOption(true);

    //solr privilege
    MSentryGMPrivilege solrPrivilege = new MSentryGMPrivilege();
    solrPrivilege.setComponentName("solr");
    solrPrivilege.setServiceName("solr.server1");
    solrPrivilege.setAuthorizables(Arrays.asList(new Collection("c1")));
    solrPrivilege.setAction("query");
    solrPrivilege.setGrantOption(true);

    PersistenceManager pm = null;
    //create role1
    pm = openTransaction();
    pm.makePersistent(new MSentryRole(roleName1, System.currentTimeMillis()));
    commitTransaction(pm);

    //create role2
    pm = openTransaction();
    pm.makePersistent(new MSentryRole(roleName2, System.currentTimeMillis()));
    commitTransaction(pm);

    //grant hivePrivilege and solrPrivilege to role1 and role2
    pm = openTransaction();
    MSentryRole role1 = getMSentryRole(pm, roleName1);
    MSentryRole role2 = getMSentryRole(pm, roleName2);
    hivePrivilege.appendRole(role1);
    solrPrivilege.appendRole(role1);
    hivePrivilege.appendRole(role2);
    solrPrivilege.appendRole(role2);
    pm.makePersistent(hivePrivilege);
    pm.makePersistent(solrPrivilege);
    pm.makePersistent(role1);
    pm.makePersistent(role2);
    commitTransaction(pm);

    //check
    pm = openTransaction();
    role1 = getMSentryRole(pm, roleName1);
    pm.retrieve(role1);
    assertEquals(1, role1.getPrivileges().size());
    assertEquals(1, role1.getGmPrivileges().size());
    role2 = getMSentryRole(pm, roleName2);
    pm.retrieve(role2);
    assertEquals(1, role2.getPrivileges().size());
    assertEquals(1, role2.getGmPrivileges().size());
    commitTransaction(pm);

    //delete role
    pm = openTransaction();
    role1 = getMSentryRole(pm, roleName1);
    role1.removePrivileges();
    role1.removeGMPrivileges();
    pm.deletePersistent(role1);
    commitTransaction(pm);

    //check for privileges
    //Privileges should be present
    pm = openTransaction();
    Query query = pm.newQuery(MSentryPrivilege.class);
    List<MSentryPrivilege> results = (List<MSentryPrivilege>) query.execute();
    assertEquals(1, results.size());
    Query query1 = pm.newQuery(MSentryGMPrivilege.class);
    List<MSentryGMPrivilege> results1 = (List<MSentryGMPrivilege>) query1.execute();
    assertEquals(1, results1.size());
    commitTransaction(pm);

    //check
    pm = openTransaction();
    role1 = getMSentryRole(pm, roleName1);
    assertTrue(role1 == null);
    commitTransaction(pm);
  }
  private PersistenceManager openTransaction() {
    PersistenceManager pm = pmf.getPersistenceManager();
    Transaction currentTransaction = pm.currentTransaction();
    currentTransaction.begin();
    return pm;
  }

  private void commitTransaction(PersistenceManager pm) {
    Transaction currentTransaction = pm.currentTransaction();
    try {
      Preconditions.checkState(currentTransaction.isActive(), "Transaction is not active");
      currentTransaction.commit();
    } finally {
      pm.close();
    }
  }

  private MSentryRole getMSentryRole(PersistenceManager pm, String roleName) {
    Query query = pm.newQuery(MSentryRole.class);
    query.setFilter("this.roleName == t");
    query.declareParameters("java.lang.String t");
    query.setUnique(true);
    MSentryRole sentryRole = (MSentryRole) query.execute(roleName);
    return sentryRole;
  }


}
