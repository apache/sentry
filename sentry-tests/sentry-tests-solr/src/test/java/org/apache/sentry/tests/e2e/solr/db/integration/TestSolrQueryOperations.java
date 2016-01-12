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

import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.core.model.search.SearchConstants;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class TestSolrQueryOperations extends AbstractSolrSentryTestWithDbProvider {
  private static final Logger LOG = LoggerFactory.getLogger(TestSolrQueryOperations.class);
  private static final String TEST_COLLECTION_NAME1 = "collection1";
  private static final String COLLECTION_CONFIG_DIR = RESOURCES_DIR + File.separator + "collection1" + File.separator + "conf";

  @Test
  public void testQueryOperations() throws Exception {
    /**
     * Upload configs to ZK for create collection
     */
    uploadConfigDirToZk(COLLECTION_CONFIG_DIR);
    /**
     * create collection collection1 as admin user
     * and clean all document in the collection1
     */
    setupCollection(TEST_COLLECTION_NAME1);
    cleanSolrCollection(TEST_COLLECTION_NAME1);
    /**
     * add a new document into collection1 for testing
     */
    SolrInputDocument solrInputDoc = createSolrTestDoc();
    uploadSolrDoc(TEST_COLLECTION_NAME1, solrInputDoc);

    /**
     * user0->group0->role0
     * grant ALL privilege on collection collection1 to role0
     */
    String grantor = "user0";
    grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role0", SearchConstants.ALL);
    verifyQueryPass(grantor, TEST_COLLECTION_NAME1, ALL_DOCS);

    revokeCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role0", SearchConstants.UPDATE);
    verifyQueryPass(grantor, TEST_COLLECTION_NAME1, ALL_DOCS);

    revokeCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role0", SearchConstants.QUERY);
    verifyQueryFail(grantor, TEST_COLLECTION_NAME1, ALL_DOCS);

    /**
     * user1->group1->role1
     * grant QUERY privilege on collection collection1 to role1
     */
    grantor = "user1";
    grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role1", SearchConstants.QUERY);
    verifyQueryPass(grantor, TEST_COLLECTION_NAME1, ALL_DOCS);

    revokeCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role1", SearchConstants.QUERY);
    verifyQueryFail(grantor, TEST_COLLECTION_NAME1, ALL_DOCS);

    /**
     * user2->group2->role2
     * grant UPDATE privilege on collection collection1 to role2
     */
    grantor = "user2";
    grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role2", SearchConstants.UPDATE);
    verifyQueryFail(grantor, TEST_COLLECTION_NAME1, ALL_DOCS);

    grantCollectionPrivilege(TEST_COLLECTION_NAME1, ADMIN_USER, "role2", SearchConstants.QUERY);
    verifyQueryPass(grantor, TEST_COLLECTION_NAME1, ALL_DOCS);

    client.renamePrivilege(ADMIN_USER, COMPONENT_SOLR, CLUSTER_NAME,
        Lists.newArrayList(new Collection(TEST_COLLECTION_NAME1)),
        Lists.newArrayList(new Collection("new_" + TEST_COLLECTION_NAME1)));
    verifyQueryFail(grantor, TEST_COLLECTION_NAME1, ALL_DOCS);

    grantor = "user3";
    verifyQueryFail(grantor, TEST_COLLECTION_NAME1, ALL_DOCS);

    deleteCollection(TEST_COLLECTION_NAME1);
  }
}