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
package org.apache.access.binding.hive;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.access.binding.hive.authz.HiveAuthzBinding;
import org.apache.access.binding.hive.authz.HiveAuthzPrivileges;
import org.apache.access.binding.hive.authz.HiveAuthzPrivilegesMap;
import org.apache.access.binding.hive.conf.HiveAuthzConf;
import org.apache.access.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.apache.access.core.Database;
import org.apache.access.core.Subject;
import org.apache.access.provider.file.PolicyFiles;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.Files;

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

  // Databases
  private static final String CUSTOMER_DB = "customers";
  private static final String ANALYST_DB = "analyst";
  private static final String JUNIOR_ANALYST_DB = "junior_analyst";

  // Tables
  private static final String PURCHASES_TAB = "purchases";
  private static final String PAYMENT_TAB = "payments";

  // Entities
  private Set<ReadEntity> inputTabList = new LinkedHashSet<ReadEntity>();
  private Set<WriteEntity> outputTabList = new LinkedHashSet<WriteEntity>();
  private HiveAuthzConf authzConf =  new HiveAuthzConf();

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

  // auth bindings handler
  private HiveAuthzBinding testAuth = null;
  private File baseDir;

  @Before
  public void setUp() throws Exception {
    inputTabList.clear();
    outputTabList.clear();
    baseDir = Files.createTempDir();
    PolicyFiles.copyToDir(baseDir, RESOURCE_PATH);

    // create auth configuration
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER.getVar(),
        "org.apache.access.provider.file.LocalGroupResourceAuthorizationProvider");
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(),
        new File(baseDir, RESOURCE_PATH).getPath());
    authzConf.set(AuthzConfVars.AUTHZ_SERVER_NAME.getVar(), SERVER1);
    testAuth = new HiveAuthzBinding(authzConf);
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
  public void TestValidateSelectPrivilegesForAdmin() throws Exception {
    inputTabList.add(new ReadEntity(new Table(CUSTOMER_DB, PURCHASES_TAB)));
    testAuth.authorize(HiveOperation.QUERY, queryPrivileges, ADMIN_SUBJECT,
        null, inputTabList, outputTabList);
  }

  /**
   * validate read permission for admin on customer:purchase
   */
  @Test
  public void TestValidateSelectPrivilegesForUsers() throws Exception {
    inputTabList.add(new ReadEntity(new Table(CUSTOMER_DB, PURCHASES_TAB)));
    testAuth.authorize(HiveOperation.QUERY, queryPrivileges, ANALYST_SUBJECT,
        null, inputTabList, outputTabList);
  }

  /**
   * validate read permission for denied for junior analyst on customer:purchase
   */
  @Test(expected=AuthorizationException.class)
  public void TestValidateSelectPrivilegesRejectionForUsers() throws Exception {
    inputTabList.add(new ReadEntity(new Table(CUSTOMER_DB, PURCHASES_TAB)));
    testAuth.authorize(HiveOperation.QUERY, queryPrivileges, JUNIOR_ANALYST_SUBJECT,
        null, inputTabList, outputTabList);
  }

  /**
   * validate create table permissions for admin in customer db
   */
  @Test
  public void TestValidateCreateTabPrivilegesForAdmin() throws Exception {
    outputTabList.add(new WriteEntity(new Table(CUSTOMER_DB, PAYMENT_TAB)));
    testAuth.authorize(HiveOperation.CREATETABLE, createTabPrivileges, ADMIN_SUBJECT,
        new Database(CUSTOMER_DB), inputTabList, outputTabList);
  }

  /**
   * validate create table permissions for manager in junior_analyst sandbox db
   */
  @Test
  public void TestValidateCreateTabPrivilegesForUser() throws Exception {
    outputTabList.add(new WriteEntity(new Table(JUNIOR_ANALYST_DB, PAYMENT_TAB)));
    testAuth.authorize(HiveOperation.CREATETABLE, createTabPrivileges, MANAGER_SUBJECT,
        new Database(JUNIOR_ANALYST_DB), inputTabList, outputTabList);
  }

  /**
   * validate create table permissions denided to junior_analyst in customer db
   */
  @Test(expected=AuthorizationException.class)
  public void TestValidateCreateTabPrivilegesRejectionForUser() throws Exception {
    outputTabList.add(new WriteEntity(new Table(CUSTOMER_DB, PAYMENT_TAB)));
    testAuth.authorize(HiveOperation.CREATETABLE, createTabPrivileges, JUNIOR_ANALYST_SUBJECT,
        new Database(CUSTOMER_DB), inputTabList, outputTabList);
  }

  /**
   * validate create table permissions denided to junior_analyst in analyst sandbox db
   */
  @Test(expected=AuthorizationException.class)
  public void TestValidateCreateTabPrivilegesRejectionForUser2() throws Exception {
    outputTabList.add(new WriteEntity(new Table(ANALYST_DB, PAYMENT_TAB)));
    testAuth.authorize(HiveOperation.CREATETABLE, createTabPrivileges, JUNIOR_ANALYST_SUBJECT,
        new Database(ANALYST_DB), inputTabList, outputTabList);
  }

  /**
   * validate load permissions for admin on customer:purchases
   */
  @Test
  public void TestValidateLoadTabPrivilegesForAdmin() throws Exception {
    outputTabList.add(new WriteEntity(new Table(CUSTOMER_DB, PURCHASES_TAB)));
    testAuth.authorize(HiveOperation.LOAD, loadTabPrivileges, ADMIN_SUBJECT,
        null, inputTabList, outputTabList);
  }

  /**
   * validate load table permissions on manager for customer:purchases
   */
  @Test
  public void TestValidateLoadTabPrivilegesForUser() throws Exception {
    outputTabList.add(new WriteEntity(new Table(CUSTOMER_DB, PURCHASES_TAB)));
    testAuth.authorize(HiveOperation.LOAD, loadTabPrivileges, MANAGER_SUBJECT,
        null, inputTabList, outputTabList);
  }

  /**
   * validate load table permissions rejected for analyst on customer:purchases
   */
  @Test(expected=AuthorizationException.class)
  public void TestValidateLoadTabPrivilegesRejectionForUser() throws Exception {
    outputTabList.add(new WriteEntity(new Table(CUSTOMER_DB, PURCHASES_TAB)));
    testAuth.authorize(HiveOperation.LOAD, loadTabPrivileges, ANALYST_SUBJECT,
        null, inputTabList, outputTabList);
  }

  /**
   * validate create database permission for admin
   */
  @Test
  public void TestValidateCreateDbForAdmin() throws Exception {
    // Hive compiler doesn't capture Entities for DB operations
    testAuth.authorize(HiveOperation.CREATEDATABASE, createDbPrivileges, ADMIN_SUBJECT,
        new Database(CUSTOMER_DB), inputTabList, outputTabList);
  }

  /**
   * validate create database permission for admin
   */
  @Test(expected=AuthorizationException.class)
  public void TestValidateCreateDbRejectionForUser() throws Exception {
    // Hive compiler doesn't capture Entities for DB operations
    testAuth.authorize(HiveOperation.CREATEDATABASE, createDbPrivileges, ANALYST_SUBJECT,
        new Database(CUSTOMER_DB), inputTabList, outputTabList);
  }

  /**
   * Validate create function permission for admin (server level priviledge
   */
  @Test
  @Ignore // TODO fix functions
  public void TestValidateCreateFunctionForAdmin() throws Exception {
    testAuth.authorize(HiveOperation.CREATEFUNCTION, createFuncPrivileges, ADMIN_SUBJECT,
        null, inputTabList, outputTabList);
  }

  /**
   * Validate create function permission for admin (server level priviledge
   */
  @Test(expected=AuthorizationException.class)
  @Ignore // TODO fix functions
  public void TestValidateCreateFunctionRejectionForUser() throws Exception {
    testAuth.authorize(HiveOperation.CREATEFUNCTION, createFuncPrivileges, ANALYST_SUBJECT,
        null, inputTabList, outputTabList);
  }

}
