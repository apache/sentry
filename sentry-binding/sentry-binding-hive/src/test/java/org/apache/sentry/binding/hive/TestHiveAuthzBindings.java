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
package org.apache.sentry.binding.hive;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.binding.hive.authz.HiveAuthzBinding;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivileges;
import org.apache.sentry.binding.hive.authz.HiveAuthzPrivilegesMap;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.sentry.binding.hive.conf.InvalidConfigurationException;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.core.model.db.AccessURI;
import org.apache.sentry.core.model.db.Column;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Server;
import org.apache.sentry.core.model.db.Table;
import org.apache.sentry.core.common.utils.PolicyFiles;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;
import com.google.common.io.Resources;

/**
 * Test for hive authz bindings
 * It uses the access.provider.file.ResourceAuthorizationProvider with the
 * resource test-authz-provider.ini
 */
public class TestHiveAuthzBindings {
  private static final String RESOURCE_PATH = "test-authz-provider.ini";
  // Servers
  private static final String SERVER1 = "server1";

  // Users
  private static final Subject ADMIN_SUBJECT = new Subject("admin1");
  private static final Subject MANAGER_SUBJECT = new Subject("manager1");
  private static final Subject ANALYST_SUBJECT = new Subject("analyst1");
  private static final Subject JUNIOR_ANALYST_SUBJECT = new Subject("junior_analyst1");
  private static final Subject NO_SUCH_SUBJECT = new Subject("no such subject");

  // Databases
  private static final String CUSTOMER_DB = "customers";
  private static final String ANALYST_DB = "analyst";
  private static final String JUNIOR_ANALYST_DB = "junior_analyst";

  // Tables
  private static final String PURCHASES_TAB = "purchases";

  // Columns
  private static final String AGE_COL = "age";

  // Entities
  private List<List<DBModelAuthorizable>> inputTabHierarcyList = new ArrayList<List<DBModelAuthorizable>>();
  private List<List<DBModelAuthorizable>> outputTabHierarcyList = new ArrayList<List<DBModelAuthorizable>>();
  private HiveConf hiveConf = new HiveConf();
  private HiveAuthzConf authzConf = new HiveAuthzConf(Resources.getResource("sentry-deprecated-site.xml"));

  // Privileges
  private static final HiveAuthzPrivileges queryPrivileges =
      HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.QUERY);
  private static final HiveAuthzPrivileges createTabPrivileges =
      HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.CREATETABLE);
  private static final HiveAuthzPrivileges loadTabPrivileges =
      HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.LOAD);
  private static final HiveAuthzPrivileges createDbPrivileges =
      HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.CREATEDATABASE);
  private static final HiveAuthzPrivileges createFuncPrivileges =
      HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.CREATEFUNCTION);
  private static final HiveAuthzPrivileges alterTabPrivileges =
      HiveAuthzPrivilegesMap.getHiveAuthzPrivileges(HiveOperation.ALTERTABLE_PROPERTIES);

  // auth bindings handler
  private HiveAuthzBinding testAuth = null;
  private File baseDir;

  @Before
  public void setUp() throws Exception {
    inputTabHierarcyList.clear();
    outputTabHierarcyList.clear();
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, RESOURCE_PATH);

    // create auth configuration
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
        "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider");
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(),
        new File(baseDir, RESOURCE_PATH).getPath());
    authzConf.set(AuthzConfVars.AUTHZ_SERVER_NAME.getVar(), SERVER1);
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "true");
    testAuth = new HiveAuthzBinding(hiveConf, authzConf);
  }

  @After
  public void teardown() {
    if(baseDir != null) {
      FileUtils.deleteQuietly(baseDir);
    }
  }

  /**
   * validate read permission for admin on customer:purchase
   */
  @Test
  public void testValidateSelectPrivilegesForAdmin() throws Exception {
    inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.QUERY, queryPrivileges, ADMIN_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate read permission for admin on customer:purchase
   */
  @Test
  public void testValidateSelectPrivilegesForUsers() throws Exception {
    inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.QUERY, queryPrivileges, ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate read permission for denied for junior analyst on customer:purchase
   */
  @Test(expected=AuthorizationException.class)
  public void testValidateSelectPrivilegesRejectionForUsers() throws Exception {
    inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.QUERY, queryPrivileges, JUNIOR_ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate create table permissions for admin in customer db
   */
  @Test
  public void testValidateCreateTabPrivilegesForAdmin() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, null));
    testAuth.authorize(HiveOperation.CREATETABLE, createTabPrivileges, ADMIN_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate create table permissions for manager in junior_analyst sandbox db
   */
  @Test
  public void testValidateCreateTabPrivilegesForUser() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, JUNIOR_ANALYST_DB, null));
    testAuth.authorize(HiveOperation.CREATETABLE, createTabPrivileges, MANAGER_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate create table permissions denided to junior_analyst in customer db
   */
  @Test(expected=AuthorizationException.class)
  public void testValidateCreateTabPrivilegesRejectionForUser() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, null));
    testAuth.authorize(HiveOperation.CREATETABLE, createTabPrivileges, JUNIOR_ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate create table permissions denided to junior_analyst in analyst sandbox db
   */
  @Test(expected=AuthorizationException.class)
  public void testValidateCreateTabPrivilegesRejectionForUser2() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, ANALYST_DB, null));
    testAuth.authorize(HiveOperation.CREATETABLE, createTabPrivileges, JUNIOR_ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * Positive test case for MSCK REPAIR TABLE. User has privileges to execute the
   * operation.
   */
  @Test
  public void testMsckRepairTable() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, JUNIOR_ANALYST_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.MSCK, alterTabPrivileges, MANAGER_SUBJECT,
      inputTabHierarcyList, outputTabHierarcyList);

    // Should also succeed for the admin.
    testAuth.authorize(HiveOperation.MSCK, alterTabPrivileges, ADMIN_SUBJECT,
      inputTabHierarcyList, outputTabHierarcyList);

    // Admin can also run this against tables in the ANALYST_DB.
    inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, ANALYST_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.MSCK, alterTabPrivileges, ADMIN_SUBJECT,
      inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * Negative case for MSCK REPAIR TABLE. User should not have privileges to execute
   * the operation.
   */
  @Test(expected=AuthorizationException.class)
  public void testMsckRepairTableRejection() throws Exception {
	inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, JUNIOR_ANALYST_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.MSCK, alterTabPrivileges,
        JUNIOR_ANALYST_SUBJECT, inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate load permissions for admin on customer:purchases
   */
  @Test
  public void testValidateLoadTabPrivilegesForAdmin() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.LOAD, loadTabPrivileges, ADMIN_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate load table permissions on manager for customer:purchases
   */
  @Test
  public void testValidateLoadTabPrivilegesForUser() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.LOAD, loadTabPrivileges, MANAGER_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);  }

  /**
   * validate load table permissions rejected for analyst on customer:purchases
   */
  @Test(expected=AuthorizationException.class)
  public void testValidateLoadTabPrivilegesRejectionForUser() throws Exception {
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.LOAD, loadTabPrivileges, ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate create database permission for admin
   */
  @Test
  public void testValidateCreateDbForAdmin() throws Exception {
    inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, null, null));
    testAuth.authorize(HiveOperation.CREATEDATABASE, createDbPrivileges, ADMIN_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * validate create database permission for admin
   */
  @Test(expected=AuthorizationException.class)
  public void testValidateCreateDbRejectionForUser() throws Exception {
    // Hive compiler doesn't capture Entities for DB operations
    outputTabHierarcyList.add(buildObjectHierarchy(SERVER1, null, null));
    testAuth.authorize(HiveOperation.CREATEDATABASE, createDbPrivileges, ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * Validate create function permission for admin (server level priviledge
   */
  @Test
  public void testValidateCreateFunctionForAdmin() throws Exception {
    inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB, AGE_COL));
    inputTabHierarcyList.add(Arrays.asList(new DBModelAuthorizable[] {
        new Server(SERVER1), new AccessURI("file:///some/path/to/a/jar")
    }));
    testAuth.authorize(HiveOperation.CREATEFUNCTION, createFuncPrivileges, ADMIN_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }
  @Test
  public void testValidateCreateFunctionAppropiateURI() throws Exception {
    inputTabHierarcyList.add(Arrays.asList(new DBModelAuthorizable[] {
        new Server(SERVER1), new AccessURI("file:///path/to/some/lib/dir/my.jar")
    }));
    testAuth.authorize(HiveOperation.CREATEFUNCTION, createFuncPrivileges, ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  @Test(expected = AuthorizationException.class)
  public void testValidateCreateFunctionRejectionForUnknownUser() throws Exception {
    inputTabHierarcyList.add(Arrays.asList(new DBModelAuthorizable[] {
        new Server(SERVER1), new AccessURI("file:///path/to/some/lib/dir/my.jar")
    }));
    testAuth.authorize(HiveOperation.CREATEFUNCTION, createFuncPrivileges, NO_SUCH_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }
  @Test(expected=AuthorizationException.class)
  public void testValidateCreateFunctionRejectionForUserWithoutURI() throws Exception {
    inputTabHierarcyList.add(Arrays.asList(new DBModelAuthorizable[] {
        new Server(SERVER1), new Database(CUSTOMER_DB), new Table(AccessConstants.ALL)
    }));
    inputTabHierarcyList.add(Arrays.asList(new DBModelAuthorizable[] {
        new Server(SERVER1), new AccessURI("file:///some/path/to/a.jar")
    }));
    testAuth.authorize(HiveOperation.CREATEFUNCTION, createFuncPrivileges, ANALYST_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  /**
   * Turn on impersonation and make sure InvalidConfigurationException is thrown.
   * @throws Exception
   */
  @Test(expected=InvalidConfigurationException.class)
  public void testImpersonationRestriction() throws Exception {
    // perpare the hive and auth configs
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, true);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "Kerberos");
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "false");
    testAuth = new HiveAuthzBinding(hiveConf, authzConf);
  }

  /**
   * HiveServer2 not using string authentication, make sure InvalidConfigurationException is thrown.
   * @throws Exception
   */
  @Test(expected=InvalidConfigurationException.class)
  public void testHiveServer2AuthRestriction() throws Exception {
    // prepare the hive and auth configs
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "none");
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "false");
    testAuth = new HiveAuthzBinding(hiveConf, authzConf);
  }

  /**
   * hive.metastore.sasl.enabled != true, make sure InvalidConfigurationException is thrown.
   * @throws Exception
   */
  @Test(expected=InvalidConfigurationException.class)
  public void testHiveMetaStoreSSLConfig() throws Exception {
    // prepare the hive and auth configs
    hiveConf.setBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL, false);
    hiveConf.setBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI, true);
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "false");
    testAuth = new HiveAuthzBinding(HiveAuthzBinding.HiveHook.HiveMetaStore, hiveConf, authzConf);
  }
  /**
   * hive.metastore.execute.setugi != true, make sure InvalidConfigurationException is thrown.
   * @throws Exception
   */
  @Test(expected=InvalidConfigurationException.class)
  public void testHiveMetaStoreUGIConfig() throws Exception {
    // prepare the hive and auth configs
    hiveConf.setBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL, true);
    hiveConf.setBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI, false);
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "true");
    testAuth = new HiveAuthzBinding(HiveAuthzBinding.HiveHook.HiveMetaStore, hiveConf, authzConf);
  }

  /**
   * Turn on impersonation and make sure that the authorization fails.
   * @throws Exception
   */
  @Test
  public void testImpersonationAllowed() throws Exception {
    // perpare the hive and auth configs
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, true);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "Kerberos");
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "false");
    authzConf.set(AuthzConfVars.AUTHZ_ALLOW_HIVE_IMPERSONATION.getVar(), "true");
    testAuth = new HiveAuthzBinding(hiveConf, authzConf);

    // following check should pass, even with impersonation
    inputTabHierarcyList.add(buildObjectHierarchy(SERVER1, CUSTOMER_DB, PURCHASES_TAB));
    testAuth.authorize(HiveOperation.QUERY, queryPrivileges, ADMIN_SUBJECT,
        inputTabHierarcyList, outputTabHierarcyList);
  }

  private List <DBModelAuthorizable>  buildObjectHierarchy(String server, String db, String table) {
    List <DBModelAuthorizable> authList = new ArrayList<DBModelAuthorizable> ();
    authList.add(new Server(server));
    if (db != null) {
      authList.add(new Database(db));
      if (table != null) {
        authList.add(new Table(table));
      }
    }
    return authList;
  }

  private List <DBModelAuthorizable>  buildObjectHierarchy(String server, String db, String table, String column) {
    List <DBModelAuthorizable> authList = buildObjectHierarchy(server, db, table);
    if (server != null && db != null && table != null && column != null) {
      authList.add(new Column(column));
    }
    return authList;
  }

  /**
   * Turn off authentication and verify exception is raised in non-testing mode
   * @throws Exception
   */
  @Test(expected=InvalidConfigurationException.class)
  public void testNoAuthenticationRestriction() throws Exception {
    // perpare the hive and auth configs
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "None");
    authzConf.set(AuthzConfVars.SENTRY_TESTING_MODE.getVar(), "false");
    testAuth = new HiveAuthzBinding(hiveConf, authzConf);
  }

  /**
   * Verify that an existing definition of only the AuthorizationProvider
   * (not ProviderBackend or PolicyEngine) still works.
   */
  @Test
  public void testDeprecatedHiveAuthzConfs() throws Exception {
    // verify that a non-existant AuthorizationProvider throws an Exception
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
      "org.apache.sentry.provider.BogusProvider");
    try {
      new HiveAuthzBinding(hiveConf, authzConf);
      Assert.fail("Expected exception");
    } catch (ClassNotFoundException e) {}

    // verify HadoopGroupResourceAuthorizationProvider
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
      "org.apache.sentry.provider.file.HadoopGroupResourceAuthorizationProvider");
    new HiveAuthzBinding(hiveConf, authzConf);

    // verify LocalGroupResourceAuthorizationProvider
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
      "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider");
    new HiveAuthzBinding(hiveConf, authzConf);
  }
}
