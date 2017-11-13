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
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.apache.sentry.core.model.solr.SolrConstants;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.junit.Test;
import org.restlet.data.MediaType;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;

@SuppressSSL
public class TestSolrSchemaOperations extends AbstractSolrSentryTestCase {
  private static final String fieldToBeAdded = "{ " +
      "\"add-field\":{" +
      "\"name\":\"test\"," +
      "\"type\":\"string\"," +
      "\"stored\":true }}";


  @Test
  public void testSolrSchemaOperations()  throws Exception {
    String collectionName = "testSolrSchemaOperations";

    grantSchemaPrivileges(ADMIN_USER, "role0", collectionName, SolrConstants.ALL);

    grantCollectionPrivileges(ADMIN_USER, ADMIN_ROLE, collectionName, SolrConstants.UPDATE);
    createCollection(ADMIN_USER, collectionName, "cloud-managed", NUM_SERVERS, 1);

    schemaReadSuccess("user0", collectionName);
    schemaUpdateSuccess("user0", collectionName);

    revokeSchemaPrivileges(ADMIN_USER, "role0", collectionName, SolrConstants.QUERY);
    schemaReadFailure("user0", collectionName);

    revokeSchemaPrivileges(ADMIN_USER, "role0", collectionName, SolrConstants.UPDATE);
    schemaUpdateFailure("user0", collectionName);
  }


  @SuppressWarnings("rawtypes")
  protected void schemaReadSuccess( String userName, String collectionName) throws ResourceException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);

      Map result = querySchemaConfig(collectionName, "name?wt=json");
      assertEquals("minimal", readNestedElement(result, "name"));

      result = querySchemaConfig(collectionName, "version?wt=json");
      assertEquals(new Double(1.1d), (Double)readNestedElement(result, "version"));

    } finally {
      setAuthenticationUser(tmp);
    }
  }

  protected void schemaReadFailure( String userName, String collectionName) throws ResourceException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);

      querySchemaConfig(collectionName, "name?wt=json");
      fail("This schema query request should have failed with authorization error.");

    } catch (ResourceException ex) {
      assertEquals(HttpServletResponse.SC_FORBIDDEN, ex.getStatus().getCode());
    } finally {
      setAuthenticationUser(tmp);
    }
  }

  @SuppressWarnings("rawtypes")
  protected void schemaUpdateSuccess( String userName, String collectionName) throws ResourceException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);

      String url = String.format("%s/%s/schema",
          cluster.getJettySolrRunner(0).getBaseUrl().toString(), collectionName);
      ClientResource resource = new ClientResource(url);
      Map updateResp = deserialize(resource.post(fieldToBeAdded, MediaType.APPLICATION_JSON));
      assertEquals(0, (long)readNestedElement(updateResp, "responseHeader", "status"));

      Map result = querySchemaConfig(collectionName, "fields/test");
      assertEquals("test", readNestedElement(result, "field", "name"));
      assertEquals("string", readNestedElement(result, "field", "type"));
      assertEquals(true, readNestedElement(result, "field", "stored"));

    } finally {
      setAuthenticationUser(tmp);
    }
  }

  protected void schemaUpdateFailure( String userName, String collectionName) throws ResourceException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);

      String url = String.format("%s/%s/schema",
          cluster.getJettySolrRunner(0).getBaseUrl().toString(), collectionName);
      ClientResource resource = new ClientResource(url);
      resource.post(fieldToBeAdded, MediaType.APPLICATION_JSON);
      fail("This schema update request should have failed with authorization error.");

    } catch (ResourceException ex) {
      assertEquals(HttpServletResponse.SC_FORBIDDEN, ex.getStatus().getCode());
    } finally {
      setAuthenticationUser(tmp);
    }
  }

  @SuppressWarnings("rawtypes")
  protected Map querySchemaConfig(String collectionName, String reqPath) throws ResourceException, IOException {
    String url = String.format("%s/%s/schema/%s",
        cluster.getJettySolrRunner(0).getBaseUrl().toString(), collectionName, reqPath);
    System.out.println("^^^:"+url);

    Map result = deserialize(new ClientResource(url).get());
    assertEquals(0, (long)readNestedElement(result, "responseHeader", "status"));

    return result;
  }

}
