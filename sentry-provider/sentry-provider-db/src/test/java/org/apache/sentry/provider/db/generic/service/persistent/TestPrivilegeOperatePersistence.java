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
import static junit.framework.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.BitFieldAction;
import org.apache.sentry.core.common.BitFieldActionFactory;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.core.model.search.Field;
import org.apache.sentry.core.model.search.SearchConstants;
import org.apache.sentry.provider.db.SentryGrantDeniedException;
import org.apache.sentry.provider.db.generic.service.persistent.PrivilegeObject.Builder;
import org.apache.sentry.provider.file.PolicyFile;
import org.apache.sentry.service.thrift.ServiceConstants;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Sets;

/**
 * The test cases are used for search component The authorizables are COLLECTION and Field
 * The actions of search privilege are ALL,QUERY and UPDATE
 */
public class TestPrivilegeOperatePersistence extends SentryStoreIntegrationBase {
  private static final String SEARCH = "solr";
  private static final String ADMIN_USER = "solr";
  private static final String GRANT_OPTION_USER = "user_grant_option";
  private static final String[] GRANT_OPTION_GROUP = { "group_grant_option" };
  private static final String NO_GRANT_OPTION_USER = "user_no_grant_option";
  private static final String[] NO_GRANT_OPTION_GROUP = { "group_no_grant_option" };

  private static final String SERVICE = "service";
  private static final String COLLECTION_NAME = "collection1";
  private static final String NOT_COLLECTION_NAME = "not_collection1";
  private static final String FIELD_NAME = "field1";
  private static final String NOT_FIELD_NAME = "not_field1";

  @Before
  public void configure() throws Exception {
    /**
     * add the solr user to admin groups
     */
    policyFile = new PolicyFile();
    addGroupsToUser(ADMIN_USER, getAdminGroups());
    writePolicyFile();
  }

  /**
   * Grant query privilege to role r1
   */
  @Test
  public void testGrantPrivilege() throws Exception {
    testGrantPrivilege(sentryStore, SEARCH);
  }

  @Test
  public void testGrantPrivilegeTwice() throws Exception {
    String roleName = "r1";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;
    sentryStore.createRole(SEARCH, roleName, grantor);

    PrivilegeObject queryPrivilegeWithOption = new Builder()
    .setComponent(SEARCH)
    .setAction(SearchConstants.QUERY)
    .setService(SERVICE)
    .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
    .withGrantOption(true)
    .build();

    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, queryPrivilegeWithOption, grantor);
    assertEquals(1,sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)).size());
    //grant again
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, queryPrivilegeWithOption, grantor);
    assertEquals(1,sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)).size());

    PrivilegeObject queryPrivilegeWithNoOption = new Builder()
    .setComponent(SEARCH)
    .setAction(SearchConstants.QUERY)
    .setService(SERVICE)
    .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
    .withGrantOption(false)
    .build();

    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, queryPrivilegeWithNoOption, grantor);
    assertEquals(2,sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)).size());
    //grant again
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, queryPrivilegeWithNoOption, grantor);
    assertEquals(2,sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)).size());

    PrivilegeObject queryPrivilegeWithNullGrant = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .withGrantOption(null)
        .build();

    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, queryPrivilegeWithNullGrant, grantor);

    assertEquals(3,sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)).size());
    //grant again
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, queryPrivilegeWithNullGrant, grantor);
    assertEquals(3,sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)).size());

  }

  /**
   * Grant query privilege to role r1 and there is ALL privilege related this
   * collection existed
   */
  @Test
  public void testGrantPrivilegeWithAllPrivilegeExist() throws Exception {
    String roleName = "r1";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;
    PrivilegeObject allPrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.ALL)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .build();

    sentryStore.createRole(SEARCH, roleName, grantor);
    /**
     * grant all privilege to role r1
     */
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, allPrivilege, grantor);
    /**
     * check role r1 truly has the privilege been granted
     */
    assertEquals(Sets.newHashSet(allPrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)));

    PrivilegeObject queryPrivilege = new Builder(allPrivilege)
        .setAction(SearchConstants.QUERY)
        .build();

    /**
     * grant query privilege to role r1
     */
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, queryPrivilege, grantor);
    /**
     * all privilege has been existed, the query privilege will not persistent
     */
    assertEquals(Sets.newHashSet(allPrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)));
  }

  /**
   * Grant query privilege to role r1 and there are query and update privileges
   * related this collection existed
   */
  @Test
  public void testGrantALLPrivilegeWithOtherPrivilegesExist() throws Exception {
    String roleName1 = "r1";
    String roleName2 = "r2";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;

    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .build();

    PrivilegeObject updatePrivilege = new Builder(queryPrivilege)
        .setAction(SearchConstants.UPDATE)
        .build();

    sentryStore.createRole(SEARCH, roleName1, grantor);
    sentryStore.createRole(SEARCH, roleName2, grantor);
    /**
     * grant query and update privilege to role r1 and role r2
     */
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, queryPrivilege, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, updatePrivilege,grantor);
    assertEquals(Sets.newHashSet(queryPrivilege, updatePrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName1)));

    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName2, queryPrivilege, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName2, updatePrivilege,grantor);
    assertEquals(Sets.newHashSet(queryPrivilege, updatePrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName2)));

    PrivilegeObject allPrivilege = new Builder(queryPrivilege)
        .setAction(SearchConstants.ALL)
        .build();

    /**
     * grant all privilege to role r1
     */
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, allPrivilege, grantor);

    /**
     * check the query and update privileges of roleName1 will be removed because of ALl privilege
     * granted
     */
    assertEquals(Sets.newHashSet(allPrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName1)));

    /**
     * check the query and update privileges of roleName2 will not affected and exist
     */
    assertEquals(Sets.newHashSet(queryPrivilege, updatePrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName2)));
  }

  @Test
  public void testGrantRevokeCheckWithGrantOption() throws Exception {

    addGroupsToUser(GRANT_OPTION_USER, GRANT_OPTION_GROUP);
    addGroupsToUser(NO_GRANT_OPTION_USER, NO_GRANT_OPTION_GROUP);
    writePolicyFile();

    String roleName1 = "r1";
    String roleName2 = "r2";
    String grantor = "g1";
    sentryStore.createRole(SEARCH, roleName1, grantor);
    sentryStore.createRole(SEARCH, roleName2, grantor);
    /**
     * grant query privilege to role r1 with grant option
     */
    PrivilegeObject queryPrivilege1 = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .withGrantOption(true)
        .build();

    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, queryPrivilege1,
        ADMIN_USER);
    assertEquals(Sets.newHashSet(queryPrivilege1),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName1)));
    /**
     * grant query privilege to role r2 no grant option
     */
    PrivilegeObject queryPrivilege2 = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .withGrantOption(false).build();

    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName2, queryPrivilege2,
        ADMIN_USER);
    assertEquals(Sets.newHashSet(queryPrivilege2),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName2)));

    sentryStore.alterRoleAddGroups(SEARCH, roleName1,
        Sets.newHashSet(GRANT_OPTION_GROUP), grantor);
    sentryStore.alterRoleAddGroups(SEARCH, roleName2,
        Sets.newHashSet(NO_GRANT_OPTION_GROUP), grantor);

    String roleName3 = "r3";
    sentryStore.createRole(SEARCH, roleName3, grantor);
    /**
     * the user with grant option grant query privilege to rolr r3
     */
    try{
      sentryStore.alterRoleGrantPrivilege(SEARCH, roleName3, queryPrivilege1,
          GRANT_OPTION_USER);
    } catch (SentryGrantDeniedException e) {
      fail("SentryGrantDeniedException shouldn't have been thrown");
    }

    /**
     * the user with grant option revoke query privilege to rolr r3
     */
    try{
      sentryStore.alterRoleRevokePrivilege(SEARCH, roleName3, queryPrivilege1,
          GRANT_OPTION_USER);
    } catch (SentryGrantDeniedException e) {
      fail("SentryGrantDeniedException shouldn't have been thrown");
    }

    /**
     * the user with no grant option grant query privilege to rolr r3, it will
     * throw SentryGrantDeniedException
     */
    try {
      sentryStore.alterRoleGrantPrivilege(SEARCH, roleName3, queryPrivilege2,
          NO_GRANT_OPTION_USER);
      fail("SentryGrantDeniedException should have been thrown");
    } catch (SentryGrantDeniedException e) {
      //ignore the exception
    }

    /**
     * the user with no grant option revoke query privilege to rolr r3, it will
     * throw SentryGrantDeniedException
     */
    try {
      sentryStore.alterRoleGrantPrivilege(SEARCH, roleName3, queryPrivilege2,
          NO_GRANT_OPTION_USER);
      fail("SentryGrantDeniedException should have been thrown");
    } catch (SentryGrantDeniedException e) {
      //ignore the exception
    }
  }

  @Test
  public void testGrantWithGrantOption() throws Exception {

    addGroupsToUser(GRANT_OPTION_USER, GRANT_OPTION_GROUP);
    addGroupsToUser(NO_GRANT_OPTION_USER, NO_GRANT_OPTION_GROUP);
    writePolicyFile();

    String roleName1 = "r1";
    String grantor = "g1";
    sentryStore.createRole(SEARCH, roleName1, grantor);
    /**
     * grant query privilege to role r1 with grant option
     */
    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .withGrantOption(true)
        .build();

    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, queryPrivilege,ADMIN_USER);
    sentryStore.alterRoleAddGroups(SEARCH, roleName1,
        Sets.newHashSet(GRANT_OPTION_GROUP), grantor);

    /**
     * the user with grant option grant query privilege to rolr r2
     */
    String roleName2 = "r2";
    sentryStore.createRole(SEARCH, roleName2, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName2, queryPrivilege, GRANT_OPTION_USER);

    assertEquals(Sets.newHashSet(queryPrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName2)));

  }


  /**
   * Grant query and update privileges to role r1 and revoke query privilege
   * there is left update privilege related to role r1
   */
  @Test
  public void testRevokePrivilege() throws Exception {
    String roleName = "r1";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;
    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
        .build();

    PrivilegeObject updatePrivilege = new Builder(queryPrivilege)
        .setAction(SearchConstants.UPDATE)
        .build();

    sentryStore.createRole(SEARCH, roleName, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, queryPrivilege, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, updatePrivilege, grantor);

    assertEquals(Sets.newHashSet(queryPrivilege,updatePrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)));
    /**
     * revoke query privilege
     */
    sentryStore.alterRoleRevokePrivilege(SEARCH, roleName, queryPrivilege, grantor);
    assertEquals(Sets.newHashSet(updatePrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)));
  }

  /**
   * Grant query and update privileges to role r1 and revoke all privilege,
   * there is no privilege related to role r1
   */
  @Test
  public void testRevokeAllPrivilege() throws Exception {
    String roleName = "r1";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;
    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME),new Field(FIELD_NAME)))
        .build();

    PrivilegeObject updatePrivilege = new Builder(queryPrivilege)
        .setAction(SearchConstants.UPDATE)
        .build();

    sentryStore.createRole(SEARCH, roleName, grantor);

    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, queryPrivilege, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, updatePrivilege, grantor);

    assertEquals(Sets.newHashSet(queryPrivilege,updatePrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)));
    /**
     * revoke all privilege
     */
    PrivilegeObject allPrivilege = new Builder(queryPrivilege)
        .setAction(SearchConstants.ALL)
        .build();

    sentryStore.alterRoleRevokePrivilege(SEARCH, roleName, allPrivilege, grantor);

    assertEquals(Sets.newHashSet(),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)));
  }

  /**
   * Grant all privilege to role r1 and revoke query privilege
   * there is update privilege related to role r1
   */
  @Test
  public void testRevokePrivilegeWithAllPrivilegeExist() throws Exception {
    String roleName = "r1";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;
    PrivilegeObject allPrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.ALL)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
        .build();

    sentryStore.createRole(SEARCH, roleName, grantor);

    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, allPrivilege, grantor);

    assertEquals(Sets.newHashSet(allPrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)));
    /**
     * revoke update privilege
     */
    PrivilegeObject updatePrivilege = new Builder(allPrivilege)
        .setAction(SearchConstants.UPDATE)
        .build();

    PrivilegeObject queryPrivilege = new Builder(allPrivilege)
        .setAction(SearchConstants.QUERY)
        .build();

    sentryStore.alterRoleRevokePrivilege(SEARCH, roleName, updatePrivilege, grantor);

    assertEquals(Sets.newHashSet(queryPrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)));
  }

  /**
   * Grant update, query and all privilege to role r1
   * Revoke query privilege from role r1
   * there is update privilege related to role r1
   */
  @Test
  public void testRevokePrivilegeWithAllPrivilegesGranted() throws Exception {
    String roleName = "r1";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;
    PrivilegeObject allPrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.ALL)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
        .build();

    PrivilegeObject updatePrivilege = new Builder(allPrivilege)
        .setAction(SearchConstants.UPDATE)
        .build();

    PrivilegeObject queryPrivilege = new Builder(allPrivilege)
        .setAction(SearchConstants.QUERY)
        .build();

    sentryStore.createRole(SEARCH, roleName, grantor);
    //grant query to role r1
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, queryPrivilege, grantor);
    assertEquals(Sets.newHashSet(queryPrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)));

    //grant update to role r1
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, updatePrivilege, grantor);
    assertEquals(Sets.newHashSet(queryPrivilege, updatePrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)));
    /**
     * grant all action privilege to role r1, because all action includes query and update action,
     * The role r1 only has the action all privilege
     */
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, allPrivilege, grantor);
    assertEquals(Sets.newHashSet(allPrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)));
    /**
     * revoke update privilege from role r1, the query privilege has been left
     */
    sentryStore.alterRoleRevokePrivilege(SEARCH, roleName, updatePrivilege, grantor);
    assertEquals(Sets.newHashSet(queryPrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)));
  }

  @Test
  public void testRevokeParentPrivilegeWithChildsExist() throws Exception {
    String roleName = "r1";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;
    PrivilegeObject updatePrivilege1 = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.UPDATE)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
        .build();

    PrivilegeObject queryPrivilege1 = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME),new Field(FIELD_NAME)))
        .build();

    PrivilegeObject queryPrivilege2 = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(NOT_COLLECTION_NAME)))
        .build();

    sentryStore.createRole(SEARCH, roleName, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, updatePrivilege1, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, queryPrivilege1, grantor);

    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName, queryPrivilege2, grantor);

    /**
     * revoke all privilege with collection[COLLECTION_NAME=collection1] and its child privileges
     */
    PrivilegeObject allPrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.ALL)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .build();

    sentryStore.alterRoleRevokePrivilege(SEARCH, roleName, allPrivilege, grantor);
    assertEquals(Sets.newHashSet(queryPrivilege2),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName)));
  }

  @Test
  public void testRevokeWithGrantOption() throws Exception {

    addGroupsToUser(GRANT_OPTION_USER, GRANT_OPTION_GROUP);
    addGroupsToUser(NO_GRANT_OPTION_USER, NO_GRANT_OPTION_GROUP);
    writePolicyFile();

    String roleName1 = "r1";
    String grantor = "g1";
    sentryStore.createRole(SEARCH, roleName1, grantor);
    /**
     * grant query privilege to role r1 with grant option
     */
    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .withGrantOption(true)
        .build();

    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, queryPrivilege,
        ADMIN_USER);
    assertEquals(Sets.newHashSet(queryPrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName1)));

    sentryStore.alterRoleAddGroups(SEARCH, roleName1,
        Sets.newHashSet(GRANT_OPTION_GROUP), grantor);

    String roleName2 = "r2";
    sentryStore.createRole(SEARCH, roleName2, grantor);
    /**
     * the user with grant option grant query privilege to rolr r2
     */
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName2, queryPrivilege,
        GRANT_OPTION_USER);
    assertEquals(Sets.newHashSet(queryPrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName2)));

    /**
     * the user with grant option revoke query privilege to rolr r3
     */
    sentryStore.alterRoleRevokePrivilege(SEARCH, roleName2, queryPrivilege, GRANT_OPTION_USER);
    assertEquals(Sets.newHashSet(),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName2)));
  }

  @Test
  public void testDropPrivilege() throws Exception{
    String roleName1 = "r1";
    String roleName2 = "r2";
    String grantor = ADMIN_USER;

    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
        .build();

    PrivilegeObject updatePrivilege = new Builder(queryPrivilege)
        .setAction(SearchConstants.UPDATE)
        .build();

    /**
     * grant query and update privilege to role r1 and r2
     */
    sentryStore.createRole(SEARCH, roleName1, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, queryPrivilege, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, updatePrivilege, grantor);

    sentryStore.createRole(SEARCH, roleName2, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName2, queryPrivilege, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName2, updatePrivilege, grantor);

    assertEquals(Sets.newHashSet(queryPrivilege,updatePrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName1)));

    assertEquals(Sets.newHashSet(queryPrivilege,updatePrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName2)));
    /**
     * drop query privilege
     */
    sentryStore.dropPrivilege(SEARCH, queryPrivilege, grantor);

    assertEquals(Sets.newHashSet(updatePrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName1)));

    assertEquals(Sets.newHashSet(updatePrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName2)));

    /**
     * drop ALL privilege
     */
    PrivilegeObject allPrivilege = new Builder(queryPrivilege)
        .setAction(SearchConstants.ALL)
        .build();

    sentryStore.dropPrivilege(SEARCH, allPrivilege, grantor);

    assertEquals(Sets.newHashSet(),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName1)));

    assertEquals(Sets.newHashSet(),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName2)));

    /**
     * grant query and update field scope[collection1,field1] privilege to role r1
     * drop collection scope[collection1] privilege
     * there is no privilege
     */
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, queryPrivilege, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, updatePrivilege, grantor);

    PrivilegeObject parentPrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.ALL)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .build();

    sentryStore.dropPrivilege(SEARCH, parentPrivilege, grantor);
    assertEquals(Sets.newHashSet(),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName1)));
  }

  @Test
  public void testRenamePrivilege() throws Exception{
    String roleName1 = "r1";
    String roleName2 = "r2";
    String grantor = ADMIN_USER;

    List<? extends Authorizable> oldAuthoriables = Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME));
    List<? extends Authorizable> newAuthoriables = Arrays.asList(new Collection(COLLECTION_NAME), new Field(NOT_FIELD_NAME));

    PrivilegeObject oldQueryPrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(oldAuthoriables)
        .build();

    PrivilegeObject oldUpdatePrivilege = new Builder(oldQueryPrivilege)
        .setAction(SearchConstants.UPDATE)
        .build();

    PrivilegeObject oldALLPrivilege = new Builder(oldQueryPrivilege)
        .setAction(SearchConstants.ALL)
        .build();


    PrivilegeObject newQueryPrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(newAuthoriables)
        .build();

    PrivilegeObject newUpdatePrivilege = new Builder(newQueryPrivilege)
        .setAction(SearchConstants.UPDATE)
        .build();

    PrivilegeObject newALLPrivilege = new Builder(newQueryPrivilege)
        .setAction(SearchConstants.ALL)
        .build();


    /**
     * grant query and update privilege to role r1
     * grant all privilege to role r2
     */
    sentryStore.createRole(SEARCH, roleName1, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, oldQueryPrivilege, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, oldUpdatePrivilege, grantor);

    sentryStore.createRole(SEARCH, roleName2, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName2, oldALLPrivilege, grantor);

    assertEquals(Sets.newHashSet(oldQueryPrivilege,oldUpdatePrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName1)));

    assertEquals(Sets.newHashSet(oldALLPrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName2)));
    /**
     * rename old query privilege to new query privilege
     */
    sentryStore.renamePrivilege(SEARCH, SERVICE,
                                      oldAuthoriables,
                                      newAuthoriables,
                                      grantor);

    assertEquals(Sets.newHashSet(newQueryPrivilege,newUpdatePrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName1)));

    assertEquals(Sets.newHashSet(newALLPrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName2)));
    /**
     * rename collection scope[collection=collection1] privilege to [collection=not_collection1]
     * These privileges belong to collection scope[collection=collection1] will change to
     * [collection=not_collection1]
     */

    List<? extends Authorizable> newAuthoriables1 = Arrays.asList(new Collection(NOT_COLLECTION_NAME),new Field(NOT_FIELD_NAME));

    PrivilegeObject newQueryPrivilege1 = new Builder(newQueryPrivilege)
          .setAuthorizables(newAuthoriables1)
          .build();

    PrivilegeObject newUpdatePrivilege1 = new Builder(newUpdatePrivilege)
          .setAuthorizables(newAuthoriables1)
          .build();

    PrivilegeObject newALLPrivilege1 = new Builder(newALLPrivilege)
          .setAuthorizables(newAuthoriables1)
          .build();

    sentryStore.renamePrivilege(SEARCH, SERVICE,
        Arrays.asList(new Collection(COLLECTION_NAME)),
        Arrays.asList(new Collection(NOT_COLLECTION_NAME)),
        grantor);

    assertEquals(Sets.newHashSet(newQueryPrivilege1,newUpdatePrivilege1),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName1)));

    assertEquals(Sets.newHashSet(newALLPrivilege1),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName2)));
  }

  @Test
  public void testGetPrivilegesByRoleName() throws Exception {
    String roleName1 = "r1";
    String roleName2 = "r2";
    String grantor = "g1";

    PrivilegeObject queryPrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .build();

    sentryStore.createRole(SEARCH, roleName1, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, queryPrivilege,
        ADMIN_USER);

    PrivilegeObject updatePrivilege = new Builder()
        .setComponent(SEARCH)
        .setAction(SearchConstants.QUERY)
        .setService(SERVICE)
        .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
        .build();

    sentryStore.createRole(SEARCH, roleName2, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName2, updatePrivilege,
        ADMIN_USER);

    assertEquals(Sets.newHashSet(queryPrivilege,updatePrivilege),
        sentryStore.getPrivilegesByRole(SEARCH, Sets.newHashSet(roleName1,roleName2)));

  }

  @Test
  public void testGetPrivilegesByProvider() throws Exception {
    String roleName1 = "r1";
    String roleName2 = "r2";
    String roleName3 = "r3";
    String group = "g3";
    String grantor = ADMIN_USER;

    String service1 = "service1";

    PrivilegeObject queryPrivilege1 = new Builder()
         .setComponent(SEARCH)
         .setAction(SearchConstants.QUERY)
         .setService(service1)
         .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
         .build();

    PrivilegeObject updatePrivilege1 = new Builder()
         .setComponent(SEARCH)
         .setAction(SearchConstants.UPDATE)
         .setService(service1)
         .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
         .build();

    PrivilegeObject queryPrivilege2 = new Builder()
         .setComponent(SEARCH)
         .setAction(SearchConstants.QUERY)
         .setService(service1)
         .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
         .build();

    PrivilegeObject updatePrivilege2 = new Builder()
         .setComponent(SEARCH)
         .setAction(SearchConstants.UPDATE)
         .setService(service1)
         .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
         .build();

    sentryStore.createRole(SEARCH, roleName1, grantor);
    sentryStore.createRole(SEARCH, roleName2, grantor);
    sentryStore.createRole(SEARCH, roleName3, grantor);

    sentryStore.alterRoleAddGroups(SEARCH, roleName3, Sets.newHashSet(group), grantor);

    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, queryPrivilege1, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, updatePrivilege1, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName2, queryPrivilege2, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName3, updatePrivilege2, grantor);

    assertEquals(Sets.newHashSet(updatePrivilege1, queryPrivilege1),
        sentryStore.getPrivilegesByProvider(SEARCH, service1, Sets.newHashSet(roleName1), null, null));

    assertEquals(Sets.newHashSet(updatePrivilege1, queryPrivilege1, queryPrivilege2),
        sentryStore.getPrivilegesByProvider(SEARCH, service1, Sets.newHashSet(roleName1,roleName2),
            null, null));

    assertEquals(Sets.newHashSet(updatePrivilege1, queryPrivilege1, queryPrivilege2, updatePrivilege2),
        sentryStore.getPrivilegesByProvider(SEARCH, service1, Sets.newHashSet(roleName1,roleName2),
            Sets.newHashSet(group), null));

    List<? extends Authorizable> authorizables = Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME));
    assertEquals(Sets.newHashSet(updatePrivilege1, updatePrivilege2),
        sentryStore.getPrivilegesByProvider(SEARCH, service1, Sets.newHashSet(roleName1,roleName2),
            Sets.newHashSet(group), authorizables));
  }

  @Test
  public void testGetPrivilegesByAuthorizable() throws Exception {
    String roleName1 = "r1";
    String roleName2 = "r2";
    String roleName3 = "r3";
    String grantor = ADMIN_USER;

    String service1 = "service1";

    PrivilegeObject queryPrivilege1 = new Builder()
    .setComponent(SEARCH)
    .setAction(SearchConstants.QUERY)
    .setService(service1)
    .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
    .build();

    PrivilegeObject updatePrivilege1 = new Builder()
    .setComponent(SEARCH)
    .setAction(SearchConstants.UPDATE)
    .setService(service1)
    .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
    .build();

    PrivilegeObject queryPrivilege2 = new Builder()
    .setComponent(SEARCH)
    .setAction(SearchConstants.QUERY)
    .setService(service1)
    .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME)))
    .build();

    PrivilegeObject updatePrivilege2 = new Builder()
    .setComponent(SEARCH)
    .setAction(SearchConstants.UPDATE)
    .setService(service1)
    .setAuthorizables(Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME)))
    .build();

    sentryStore.createRole(SEARCH, roleName1, grantor);
    sentryStore.createRole(SEARCH, roleName2, grantor);
    sentryStore.createRole(SEARCH, roleName3, grantor);

    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, queryPrivilege1, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName1, updatePrivilege1, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName2, queryPrivilege2, grantor);
    sentryStore.alterRoleGrantPrivilege(SEARCH, roleName3, updatePrivilege2, grantor);

    assertEquals(0, sentryStore.getPrivilegesByAuthorizable(SEARCH, service1, null,
        Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME))).size());
    assertEquals(1, sentryStore.getPrivilegesByAuthorizable(SEARCH, service1, Sets.newHashSet(roleName1),
    Arrays.asList(new Collection(COLLECTION_NAME), new Field(FIELD_NAME))).size());
    assertEquals(2, sentryStore.getPrivilegesByAuthorizable(SEARCH, service1,
        Sets.newHashSet(roleName1), null).size());
    assertEquals(2, sentryStore.getPrivilegesByAuthorizable(SEARCH, service1,
        Sets.newHashSet(roleName1,roleName2), null).size());
    assertEquals(2, sentryStore.getPrivilegesByAuthorizable(SEARCH, service1,
        Sets.newHashSet(roleName1,roleName2, roleName3), null).size());
  }

  @Test(expected = Exception.class)
  public void testGrantPrivilegeExternalComponentMissingConf() throws Exception {
    testGrantPrivilege(sentryStore, "externalComponent");
  }

  @Test(expected = Exception.class)
  public void testGrantPrivilegeExternalComponentInvalidConf() throws Exception {
    String externalComponent = "mycomponent";
    Configuration confCopy = new Configuration(conf);
    confCopy.set(String.format(ServiceConstants.ServerConfig.SENTRY_COMPONENT_ACTION_FACTORY_FORMAT, externalComponent),
                 InvalidActionFactory.class.getName());
    SentryStoreLayer store = new DelegateSentryStore(confCopy);
    testGrantPrivilege(store, externalComponent);
  }

  @Test
  public void testGrantPrivilegeExternalComponent() throws Exception {
    String externalComponent = "mycomponent";
    Configuration confCopy = new Configuration(conf);
    confCopy.set(String.format(ServiceConstants.ServerConfig.SENTRY_COMPONENT_ACTION_FACTORY_FORMAT, externalComponent),
                 MyComponentActionFactory.class.getName());
    SentryStoreLayer store = new DelegateSentryStore(confCopy);
    testGrantPrivilege(store, externalComponent);
  }

  @Test
  public void testGrantPrivilegeExternalComponentCaseInsensitivity() throws Exception {
    String externalComponent = "MyCoMpOnEnT";
    Configuration confCopy = new Configuration(conf);
    confCopy.set(String.format(ServiceConstants.ServerConfig.SENTRY_COMPONENT_ACTION_FACTORY_FORMAT, "mycomponent"),
                 MyComponentActionFactory.class.getName());
    SentryStoreLayer store = new DelegateSentryStore(confCopy);
    testGrantPrivilege(store, externalComponent);
  }

  private void testGrantPrivilege(SentryStoreLayer sentryStore, String component) throws Exception {
    String roleName = "r1";
    /**
     * grantor is admin, there is no need to check grant option
     */
    String grantor = ADMIN_USER;
    PrivilegeObject queryPrivilege = new Builder()
      .setComponent(component)
      .setAction(SearchConstants.QUERY)
      .setService(SERVICE)
      .setAuthorizables(Collections.singletonList(new Collection(COLLECTION_NAME)))
      .withGrantOption(null)
      .build();

    sentryStore.createRole(component, roleName, grantor);
    sentryStore.alterRoleGrantPrivilege(component, roleName, queryPrivilege, grantor);

    assertEquals(Sets.newHashSet(queryPrivilege),
                 sentryStore.getPrivilegesByRole(component, Sets.newHashSet(roleName)));

    PrivilegeObject queryPrivilegeWithOption = new Builder()
      .setComponent(component)
      .setAction(SearchConstants.QUERY)
      .setService(SERVICE)
      .setAuthorizables(Collections.singletonList(new Collection(COLLECTION_NAME)))
      .withGrantOption(true)
      .build();

    sentryStore.alterRoleGrantPrivilege(component, roleName, queryPrivilegeWithOption, grantor);

    assertEquals(Sets.newHashSet(queryPrivilege, queryPrivilegeWithOption),
                 sentryStore.getPrivilegesByRole(component, Sets.newHashSet(roleName)));

    PrivilegeObject queryPrivilegeWithNoOption = new Builder()
      .setComponent(component)
      .setAction(SearchConstants.QUERY)
      .setService(SERVICE)
      .setAuthorizables(Collections.singletonList(new Collection(COLLECTION_NAME)))
      .withGrantOption(false)
      .build();

    sentryStore.alterRoleGrantPrivilege(component, roleName, queryPrivilegeWithNoOption, grantor);

    assertEquals(Sets.newHashSet(queryPrivilege, queryPrivilegeWithOption, queryPrivilegeWithNoOption),
                 sentryStore.getPrivilegesByRole(component, Sets.newHashSet(roleName)));
  }

  public static final class InvalidActionFactory {

  }

  public static final class MyComponentActionFactory extends BitFieldActionFactory {

    public enum MyComponentActionType {
      FOO("foo", 1),
      BAR("bar", 2),
      QUERY(SearchConstants.QUERY, 4),
      ALL("*", FOO.getCode() | BAR.getCode() | QUERY.getCode());

      private String name;
      private int code;
      MyComponentActionType(String name, int code) {
        this.name = name;
        this.code = code;
      }

      public int getCode() {
        return code;
      }

      public String getName() {
        return name;
      }

      static MyComponentActionType getActionByName(String name) {
        for (MyComponentActionType action : MyComponentActionType.values()) {
          if (action.name.equalsIgnoreCase(name)) {
            return action;
          }
        }
        throw new RuntimeException("can't get MyComponentActionType by name:" + name);
      }

      static List<MyComponentActionType> getActionByCode(int code) {
        List<MyComponentActionType> actions = Lists.newArrayList();
        for (MyComponentActionType action : MyComponentActionType.values()) {
          if ((action.code & code) == action.code && action != MyComponentActionType.ALL) {
            //MyComponentActionType.ALL action should not return in the list
            actions.add(action);
          }
        }
        if (actions.isEmpty()) {
          throw new RuntimeException("can't get sqoopActionType by code:" + code);
        }
        return actions;
      }
    }

    public static class MyComponentAction extends BitFieldAction {
      public MyComponentAction(String name) {
        this(MyComponentActionType.getActionByName(name));
      }
      public MyComponentAction(MyComponentActionType myComponentActionType) {
        super(myComponentActionType.name, myComponentActionType.code);
      }
    }

    @Override
    public List<? extends BitFieldAction> getActionsByCode(int actionCode) {
      List<MyComponentAction> actions = Lists.newArrayList();
      for (MyComponentActionType action : MyComponentActionType.getActionByCode(actionCode)) {
        actions.add(new MyComponentAction(action));
      }
      return actions;
    }

    @Override
    public BitFieldAction getActionByName(String name) {
      // Check the name is All
      if (Action.ALL.equalsIgnoreCase(name)) {
        return new MyComponentAction(MyComponentActionType.ALL);
      }
      return new MyComponentAction(name);
    }
  }
}
