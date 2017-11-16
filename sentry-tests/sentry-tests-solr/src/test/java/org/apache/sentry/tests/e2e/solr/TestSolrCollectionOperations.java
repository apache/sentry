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
package org.apache.sentry.tests.e2e.solr;

import static org.apache.sentry.tests.e2e.solr.TestSentryServer.ADMIN_USER;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import org.apache.sentry.core.model.solr.SolrConstants;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.junit.Test;


public class TestSolrCollectionOperations extends AbstractSolrSentryTestCase {

  @Test
  public void testQueryOperations() throws Exception {
    String collectionName = "testCollectionQueryOps";

    // Create collection as an admin user.
    grantCollectionPrivileges(ADMIN_USER, ADMIN_ROLE, collectionName, SolrConstants.UPDATE);
    createCollection(ADMIN_USER, collectionName, "cloud-minimal", NUM_SERVERS, 1);

    // Grant all privileges for the test collection to role0
    grantCollectionPrivileges(ADMIN_USER, "role0", collectionName, SolrConstants.ALL);

    cleanSolrCollection("user0", collectionName);

    SolrInputDocument solrInputDoc = createSolrTestDoc();
    uploadSolrDoc("user0", collectionName, solrInputDoc);
    SolrDocumentList expectedDocs = expectedDocs(solrInputDoc);

    validateSolrDocCountAndContent(expectedDocs,
        getSolrDocs("user0", collectionName, ALL_DOCS));

    revokeCollectionPrivileges(ADMIN_USER, "role0", collectionName, SolrConstants.UPDATE);
    validateSolrDocCountAndContent(expectedDocs,
        getSolrDocs("user0", collectionName, ALL_DOCS));

    revokeCollectionPrivileges(ADMIN_USER, "role0", collectionName, SolrConstants.QUERY);
    verifyCollectionQueryFailure("user0", collectionName, ALL_DOCS);

    verifyCollectionQueryFailure("user1", collectionName, ALL_DOCS);
  }

  @Test
  public void testUpdateOperations() throws Exception {
    String collectionName = "testCollectionUpdateOps";

    // Create collection as an admin user.
    grantCollectionPrivileges(ADMIN_USER, ADMIN_ROLE, collectionName, SolrConstants.UPDATE);
    createCollection(ADMIN_USER, collectionName, "cloud-minimal", 1, NUM_SERVERS);

    // Grant all privileges for the test collection to role0
    grantCollectionPrivileges(ADMIN_USER, "role0", collectionName, SolrConstants.ALL);

    cleanSolrCollection("user0", collectionName);

    SolrInputDocument solrInputDoc = createSolrTestDoc();
    uploadSolrDoc("user0", collectionName, solrInputDoc);
    SolrDocumentList expectedDocs = expectedDocs(solrInputDoc);

    validateSolrDocCountAndContent(expectedDocs,
        getSolrDocs("user0", collectionName, ALL_DOCS));

    verifyDeletedocsPass("user0", collectionName, false);

    revokeCollectionPrivileges(ADMIN_USER, "role0", collectionName, SolrConstants.ALL);

    verifyCollectionUpdateFailure("user0", collectionName, solrInputDoc);
    verifyCollectionUpdateFailure("user1", collectionName, solrInputDoc);
  }


  protected void verifyCollectionUpdateFailure(String userName, String collectionName,
      SolrInputDocument doc) throws SolrServerException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);
      cluster.getSolrClient().add(collectionName, doc);
      cluster.getSolrClient().commit(collectionName);
      fail("This collection query request should have failed with authorization error.");

    } catch (RemoteSolrException ex) {
      assertEquals(HttpServletResponse.SC_FORBIDDEN, ex.code());
    } finally {
      setAuthenticationUser(tmp);
    }
  }

  protected void verifyCollectionQueryFailure(String userName, String collectionName,
      String queryStr) throws SolrServerException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);
      cluster.getSolrClient().query(collectionName, new SolrQuery(queryStr));
      fail("This collection query request should have failed with authorization error.");

    } catch (SolrServerException ex) {
      assertTrue(ex.getRootCause() instanceof RemoteSolrException);
      assertEquals(HttpServletResponse.SC_FORBIDDEN, ((RemoteSolrException)ex.getRootCause()).code());
    } finally {
      setAuthenticationUser(tmp);
    }
  }

  protected SolrDocumentList expectedDocs(SolrInputDocument... docs) {
    SolrDocumentList result = new SolrDocumentList();

    for (SolrInputDocument doc : docs) {
      SolrDocument r = new SolrDocument();
      for (SolrInputField field : doc) {
        r.setField(field.getName(), field.getValue());
      }
      result.add(r);
    }
    return result;
  }
}
