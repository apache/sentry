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
package org.apache.sentry.tests.e2e.solr.db.integration;


import java.io.File;
import java.util.Arrays;

import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.core.model.search.SearchConstants;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSolrAdminOperations extends AbstractSolrSentryTestWithDbProvider {
  private static final Logger LOG = LoggerFactory.getLogger(TestSolrAdminOperations.class);
  private static final String TEST_COLLECTION_NAME1 = "collection1";
  private static final String COLLECTION_CONFIG_DIR = RESOURCES_DIR + File.separator + "collection1" + File.separator + "conf";

  @Test
  public void testAdminOperations() throws Exception {
    /**
     * Upload configs to ZK for create collection
     */
    uploadConfigDirToZk(COLLECTION_CONFIG_DIR);

    /**
     * verify admin user has all privileges
     */
    verifyCollectionAdminOpPass(ADMIN_USER, CollectionAction.CREATE, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(ADMIN_USER, CollectionAction.DELETE, TEST_COLLECTION_NAME1);

    String grantor = "user0";
    /**
     * user0->group0->role0
     * grant ALL privilege on collection admin and collection1 to role0
     */
    grantCollectionPrivilege(ADMIN_COLLECTION_NAME, ADMIN_USER, "role0", SearchConstants.ALL);
    grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role0", SearchConstants.ALL);

    verifyCollectionAdminOpPass(grantor, CollectionAction.CREATE, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.RELOAD, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.CREATEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.DELETEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.DELETE, TEST_COLLECTION_NAME1);

    //revoke UPDATE privilege on collection collection1 from role1, create collection1 will be failed
    revokeCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role0", SearchConstants.UPDATE);

    verifyCollectionAdminOpFail(grantor, CollectionAction.CREATE, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.RELOAD, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.CREATEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.DELETEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.DELETE, TEST_COLLECTION_NAME1);

    /**
     * user1->group1->role1
     * grant UPDATE privilege on collection admin and collection1 to role1
     */
    grantor = "user1";
    grantCollectionPrivilege(ADMIN_COLLECTION_NAME, ADMIN_USER, "role1", SearchConstants.UPDATE);
    grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role1", SearchConstants.UPDATE);

    verifyCollectionAdminOpPass(grantor, CollectionAction.CREATE, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.RELOAD, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.CREATEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.DELETEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.DELETE, TEST_COLLECTION_NAME1);

    //revoke UPDATE privilege on collection admin from role1, create collection1 will be failed
    revokeCollectionPrivilege(ADMIN_COLLECTION_NAME, ADMIN_USER, "role1", SearchConstants.UPDATE);
    verifyCollectionAdminOpFail(grantor, CollectionAction.CREATE, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.RELOAD, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.CREATEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.DELETEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.DELETE, TEST_COLLECTION_NAME1);


    /**
     * user2->group2->role2
     * grant QUERY privilege on collection admin and collection1 to role2
     */
    grantor = "user2";
    grantCollectionPrivilege(ADMIN_COLLECTION_NAME, ADMIN_USER, "role2", SearchConstants.QUERY);
    grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role2", SearchConstants.QUERY);

    verifyCollectionAdminOpFail(grantor, CollectionAction.CREATE, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.RELOAD, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.CREATEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.DELETEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.DELETE, TEST_COLLECTION_NAME1);

    //grant UPDATE privilege on collection collection1 to role2, create collection1 will be failed
    grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role2", SearchConstants.UPDATE);
    verifyCollectionAdminOpFail(grantor, CollectionAction.CREATE, TEST_COLLECTION_NAME1);

    //grant UPDATE privilege on collection admin to role2, create collection1 will be successful.
    grantCollectionPrivilege(ADMIN_COLLECTION_NAME, ADMIN_USER, "role2", SearchConstants.UPDATE);

    verifyCollectionAdminOpPass(grantor, CollectionAction.CREATE, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.RELOAD, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.CREATEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.DELETEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.DELETE, TEST_COLLECTION_NAME1);

    grantor = "user3";

    verifyCollectionAdminOpFail(grantor, CollectionAction.CREATE, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.RELOAD, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.CREATEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.DELETEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpFail(grantor, CollectionAction.DELETE, TEST_COLLECTION_NAME1);

    /**
     * user3->group3->role3
     * grant UPDATE privilege on collection admin to role3
     * grant QUERY privilege on collection collection1 to role3
     */
    grantCollectionPrivilege(ADMIN_COLLECTION_NAME, ADMIN_USER, "role3", SearchConstants.ALL);
    grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role3", SearchConstants.ALL);

    verifyCollectionAdminOpPass(grantor, CollectionAction.CREATE, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.RELOAD, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.CREATEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.DELETEALIAS, TEST_COLLECTION_NAME1);
    verifyCollectionAdminOpPass(grantor, CollectionAction.DELETE, TEST_COLLECTION_NAME1);
  }

  /**
   * Test when the collection has been deleted, the privileges in the sentry service also should be deleted
   * @throws Exception
   */
  @Ignore("CDH-36881: Disable automatic permission revoke on collection/config deletion")
  @Test
  public void testSyncPrivilegesWithDeleteCollection() throws Exception {
    /**
     * Upload configs to ZK for create collection
     */
    uploadConfigDirToZk(COLLECTION_CONFIG_DIR);
    /**
     * user0->group0->role0
     * Grant ALL privilege on collection collection1 to role0
     * Grant ALL privilege on collection admin to role0
     * user0 can execute create & delete collection1 operation
     */
    grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role0", SearchConstants.ALL);
    grantCollectionPrivilege(ADMIN_COLLECTION_NAME, ADMIN_USER, "role0", SearchConstants.ALL);

    assertTrue("user0 has one privilege on collection admin",
        client.listPrivilegesByRoleName("user0", "role0", COMPONENT_SOLR, SERVICE_NAME,
            Arrays.asList(new Collection(ADMIN_COLLECTION_NAME))).size() == 1);

    assertTrue("user0 has one privilege on collection collection1",
        client.listPrivilegesByRoleName("user0", "role0", COMPONENT_SOLR, SERVICE_NAME,
            Arrays.asList(new Collection(TEST_COLLECTION_NAME1))).size() == 1);

    /**
     * user1->group1->role1
     * grant QUERY privilege on collection collection1 to role1
     */

    client.listPrivilegesByRoleName("user0", "role0", COMPONENT_SOLR, SERVICE_NAME, null);
    grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role1", SearchConstants.ALL);
    assertTrue("user1 has one privilege record",
        client.listPrivilegesByRoleName("user1", "role1", COMPONENT_SOLR, SERVICE_NAME,
            Arrays.asList(new Collection(TEST_COLLECTION_NAME1))).size() == 1);

    /**
     * create collection collection1
     */
    setupCollection(TEST_COLLECTION_NAME1);
    /**
     * delete the collection1
     */
    deleteCollection(TEST_COLLECTION_NAME1);

    //check the user0
    assertTrue("user0 has one privilege on collection admin",
        client.listPrivilegesByRoleName("user0", "role0", COMPONENT_SOLR, SERVICE_NAME,
            Arrays.asList(new Collection(ADMIN_COLLECTION_NAME))).size() == 1);

    assertTrue("user0 has no privilege on collection collection1",
        client.listPrivilegesByRoleName("user0", "role0", COMPONENT_SOLR, SERVICE_NAME,
            Arrays.asList(new Collection(TEST_COLLECTION_NAME1))).size() == 0);

    //check the user1
    assertTrue("user1 has no privilege on collection collection1",
        client.listPrivilegesByRoleName("user1", "role1", COMPONENT_SOLR, SERVICE_NAME,
            Arrays.asList(new Collection(TEST_COLLECTION_NAME1))).size() == 0);

    /**
     * user2->group2->role2
     * Grant UPDATE privilege on collection collection1 to role2
     */
    grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role2", SearchConstants.UPDATE);

    assertTrue("user2 has one privilege on collection collection1",
        client.listPrivilegesByRoleName("user2", "role2", COMPONENT_SOLR, SERVICE_NAME,
            Arrays.asList(new Collection(TEST_COLLECTION_NAME1))).size() == 1);

    /**
     * user3->group3->role3
     * grant QUERY privilege on collection collection1 to role3
     */
    grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role3", SearchConstants.QUERY);
    assertTrue("user1 has one privilege record",
        client.listPrivilegesByRoleName("user3", "role3", COMPONENT_SOLR, SERVICE_NAME,
            Arrays.asList(new Collection(TEST_COLLECTION_NAME1))).size() == 1);

    /**
     * create collection collection1
     */
    setupCollection(TEST_COLLECTION_NAME1);
    /**
     * delete the collection1
     */
    deleteCollection(TEST_COLLECTION_NAME1);

    //check the user2
    assertTrue("user2 has no privilege on collection collection1",
        client.listPrivilegesByRoleName("user2", "role2", COMPONENT_SOLR, SERVICE_NAME,
            Arrays.asList(new Collection(TEST_COLLECTION_NAME1))).size() == 0);

    //check the user3
    assertTrue("user3 has no privilege on collection collection1",
        client.listPrivilegesByRoleName("user3", "role3", COMPONENT_SOLR, SERVICE_NAME,
            Arrays.asList(new Collection(TEST_COLLECTION_NAME1))).size() == 0);
  }
}