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
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.response.ConfigSetAdminResponse;
import org.junit.Test;
import org.restlet.data.MediaType;
import org.restlet.resource.ClientResource;
import org.restlet.resource.ResourceException;

@SuppressSSL
public class TestSolrConfigOperations extends AbstractSolrSentryTestCase {
  @Test
  public void testConfigSetOperations() throws Exception {
    grantConfigPrivileges(ADMIN_USER, "role0", "test", SolrConstants.UPDATE);
    grantConfigPrivileges(ADMIN_USER, "role1", SolrConstants.ALL, SolrConstants.QUERY);

    configSetCreationSuccess("user0", "test", "cloud-minimal");

    ConfigSetAdminResponse resp = configSetListSuccess("user1");
    assertEquals(2, resp.getResponse().size());

    configSetDeleteSuccess("user0", "test");

    configSetCreationFailure("user1", "test1", "cloud-minimal");
    configSetListFailure("user0");
  }

  @Test
  public void testConfigOperations() throws Exception {
    String collectionName = "testConfigOperations";

    grantConfigPrivileges(ADMIN_USER, "role0", collectionName, SolrConstants.ALL);
    grantConfigPrivileges(ADMIN_USER, "role1", SolrConstants.ALL, SolrConstants.QUERY);

    grantCollectionPrivileges(ADMIN_USER, ADMIN_ROLE, collectionName, SolrConstants.UPDATE);
    createCollection(ADMIN_USER, collectionName, "cloud-minimal", NUM_SERVERS, 1);

    configReadSuccess("user1", collectionName);
    configReadSuccess("user0", collectionName);
    configUpdateSuccess("user0", collectionName);

    revokeConfigPrivileges(ADMIN_USER, "role0", collectionName, SolrConstants.QUERY);
    configReadFailure ("user0", collectionName);
  }

  @SuppressWarnings("rawtypes")
  protected void configReadSuccess( String userName, String collectionName)
      throws ResourceException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);

      Map result = queryHandlerConfig(collectionName, "/select");
      assertEquals("/select", (String)readNestedElement(result,
          "config", "requestHandler", "/select", "name"));
      assertEquals("solr.SearchHandler", (String)readNestedElement(result,
          "config", "requestHandler", "/select", "class"));
      assertEquals("explicit", (String)readNestedElement(result,
          "config", "requestHandler", "/select", "defaults", "echoParams"));

    } finally {
      setAuthenticationUser(tmp);
    }
  }

  protected void configReadFailure( String userName, String collectionName)
      throws ResourceException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);

      queryHandlerConfig(collectionName, "/select");
      fail("This config query request should have failed with authorization error.");

    } catch (ResourceException ex) {
      assertEquals(HttpServletResponse.SC_FORBIDDEN, ex.getStatus().getCode());
    } finally {
      setAuthenticationUser(tmp);
    }
  }

  @SuppressWarnings("rawtypes")
  protected void configUpdateSuccess( String userName, String collectionName)
      throws ResourceException, IOException {

    String configToBeAdded = "{" +
                                "\"add-requesthandler\" : { " +
                                "\"name\": \"/mypath\"," +
                                "\"class\":\"solr.DumpRequestHandler\", " +
                                "\"defaults\":{ \"x\":\"y\" ,\"a\":\"b\"," +
                                " \"wt\":\"json\", \"indent\":true }," +
                                "\"useParams\":\"x\"}}";

    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);
      String url = String.format("%s/%s/config",
          cluster.getJettySolrRunner(0).getBaseUrl().toString(), collectionName);

      ClientResource resource = new ClientResource(url);
      Map updateResp = deserialize(resource.post(configToBeAdded, MediaType.APPLICATION_JSON));
      assertEquals(0, (long)readNestedElement(updateResp, "responseHeader", "status"));

      Map result = queryHandlerConfig(collectionName, "/mypath");
      assertEquals("/mypath", (String)readNestedElement(result,
          "config", "requestHandler", "/mypath", "name"));
      assertEquals("solr.DumpRequestHandler", (String)readNestedElement(result,
          "config", "requestHandler", "/mypath", "class"));
      assertEquals("y", (String)readNestedElement(result,
          "config", "requestHandler", "/mypath", "defaults", "x"));

    } finally {
      setAuthenticationUser(tmp);
    }
  }

  protected void configSetCreationSuccess(String userName, String configName,
      String baseConfigSetName) throws SolrServerException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);
      ConfigSetAdminRequest.Create create = new ConfigSetAdminRequest.Create();
      create.setBaseConfigSetName(baseConfigSetName);
      create.setConfigSetName(configName);
      assertEquals(0, create.process(cluster.getSolrClient()).getStatus());

    } finally {
      setAuthenticationUser(tmp);
    }
  }

  protected void configSetCreationFailure(String userName, String configName,
      String baseConfigSetName) throws SolrServerException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);
      ConfigSetAdminRequest.Create create = new ConfigSetAdminRequest.Create();
      create.setBaseConfigSetName(baseConfigSetName);
      create.setConfigSetName(configName);
      create.process(cluster.getSolrClient());
      fail("This config request should have failed with authorization error.");

    } catch (RemoteSolrException ex) {
      assertEquals(HttpServletResponse.SC_FORBIDDEN , ex.code());
    } finally {
      setAuthenticationUser(tmp);
    }
  }

  protected void configSetDeleteSuccess(String userName, String configName)
      throws SolrServerException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);
      ConfigSetAdminRequest.Delete delete = new ConfigSetAdminRequest.Delete();
      delete.setConfigSetName(configName);
      assertEquals(0, delete.process(cluster.getSolrClient()).getStatus());

    } finally {
      setAuthenticationUser(tmp);
    }
  }

  protected ConfigSetAdminResponse configSetListSuccess(String userName)
      throws SolrServerException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);
      ConfigSetAdminRequest.List list = new ConfigSetAdminRequest.List();
      ConfigSetAdminResponse resp = list.process(cluster.getSolrClient());
      assertEquals(0, resp.getStatus());
      return resp;

    } finally {
      setAuthenticationUser(tmp);
    }
  }

  protected void configSetListFailure(String userName)
      throws SolrServerException, IOException {
    String tmp = getAuthenticatedUser();
    try {
      setAuthenticationUser(userName);
      ConfigSetAdminRequest.List list = new ConfigSetAdminRequest.List();
      list.process(cluster.getSolrClient());
      fail("This config request should have failed with authorization error.");

    } catch (RemoteSolrException ex) {
      assertEquals(HttpServletResponse.SC_FORBIDDEN , ex.code());
    } finally {
      setAuthenticationUser(tmp);
    }
  }

  @SuppressWarnings("rawtypes")
  protected Map queryHandlerConfig(String collectionName, String handlerPath) throws ResourceException, IOException {
    String url = String.format("%s/%s/config/requestHandler?componentName=%s",
        cluster.getJettySolrRunner(0).getBaseUrl().toString(), collectionName, handlerPath);

    Map result = deserialize(new ClientResource(url).get());
    assertEquals(0, (long)readNestedElement(result, "responseHeader", "status"));

    return result;
  }
}
