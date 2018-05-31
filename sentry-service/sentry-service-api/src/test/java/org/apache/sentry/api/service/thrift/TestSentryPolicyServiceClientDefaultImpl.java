/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.api.service.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.api.common.Status;
import org.apache.sentry.api.service.thrift.SentryPolicyService.Client;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.exception.SentryAccessDeniedException;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import org.apache.sentry.core.model.db.Table;

public class TestSentryPolicyServiceClientDefaultImpl {
  private final static Client mockClient = Mockito.mock(Client.class);

  private SentryPolicyServiceClientDefaultImpl sentryClient;

  @Before
  public void setup() throws IOException {
    Mockito.reset(mockClient);

    // Initialize the mock for the Sentry client
    Configuration conf = new Configuration();
    sentryClient = new SentryPolicyServiceClientDefaultImpl(conf, null);
    sentryClient.setClient(mockClient);
  }

  @Test
  public void testListAllPrivilegesByUserName() throws SentryUserException, TException {
    Set<TSentryPrivilege> allPrivileges;

    // Prepare some privileges for user1
    Mockito.when(mockClient.list_sentry_privileges_by_user(
      listSentryPrivilegesRequest("admin", "user1", null)))
      .thenReturn(listSentryPrivilegesResponse(
        Sets.newHashSet(
          newSentryPrivilege("database", "db1", "t1", "select"),
          newSentryPrivilege("database", "db1", "t2", "insert"))));

    // Prepare some privileges for user2
    Mockito.when(mockClient.list_sentry_privileges_by_user(
      listSentryPrivilegesRequest("admin", "user2", null)))
      .thenReturn(listSentryPrivilegesResponse(
        Sets.newHashSet(
          newSentryPrivilege("database", "db1", "t1", "*"),
          newSentryPrivilege("database", "db1", "t2", "*"))));

    // Request all privileges as user1
    allPrivileges = sentryClient.listAllPrivilegesByUserName("admin", "user1");
    assertEquals(2, allPrivileges.size());
    assertTrue(allPrivileges.contains(newSentryPrivilege("database", "db1", "t1", "select")));
    assertTrue(allPrivileges.contains(newSentryPrivilege("database", "db1", "t2", "insert")));

    // Request all privileges as user2
    allPrivileges = sentryClient.listAllPrivilegesByUserName("admin", "user2");
    assertEquals(2, allPrivileges.size());
    assertTrue(allPrivileges.contains(newSentryPrivilege("database", "db1", "t1", "*")));
    assertTrue(allPrivileges.contains(newSentryPrivilege("database", "db1", "t2", "*")));

    // Prepare some privileges for user1 as requestor
    TListSentryPrivilegesResponse accessDeniedResp = new TListSentryPrivilegesResponse();
    accessDeniedResp.setStatus(Status.AccessDenied("", new SentryAccessDeniedException("")));
    Mockito.when(mockClient.list_sentry_privileges_by_user(
      listSentryPrivilegesRequest("user1", "user2", null)))
      .thenReturn(accessDeniedResp);

    // Request all privileges as unauthorized user
    try {
      sentryClient.listAllPrivilegesByUserName("user1", "user2");
      assertTrue("Requesting privileges as a unauthorized user should fail", false);
    } catch (SentryAccessDeniedException e) {
      assertTrue(true);
    }
  }

  @Test
  public void testListPrivilegesByUserName() throws SentryUserException, TException {
    Set<TSentryPrivilege> privileges;

    // Prepare some privileges for user1
    Mockito.when(mockClient.list_sentry_privileges_by_user(
      listSentryPrivilegesRequest("admin", "user1", Arrays.asList(new Table("t1")))))
      .thenReturn(listSentryPrivilegesResponse(
        Sets.newHashSet(
          newSentryPrivilege("database", "db1", "t1", "select")
        )));

    // Request all privileges as user1
    privileges = sentryClient.listPrivilegesByUserName("admin", "user1", Arrays.asList(new Table("t1")));
    assertEquals(1, privileges.size());
    assertTrue(privileges.contains(newSentryPrivilege("database", "db1", "t1", "select")));
  }

  private static TSentryPrivilege newSentryPrivilege(String scope, String dbname, String tablename, String action) {
    TSentryPrivilege privilege = new TSentryPrivilege();
    privilege.setPrivilegeScope(scope);
    privilege.setDbName(dbname);
    privilege.setTableName(tablename);
    privilege.setAction(action);
    return privilege;
  }

  private static TListSentryPrivilegesRequest listSentryPrivilegesRequest(String requestorUser, String entityName, List<? extends Authorizable> authorizable) {
    return Mockito.argThat(new ArgumentMatcher<TListSentryPrivilegesRequest>() {
      @Override
      public boolean matches(Object o) {
        if (o == null) {
          return false;
        }

        TListSentryPrivilegesRequest request = (TListSentryPrivilegesRequest)o;
        if (authorizable != null && !authorizable.isEmpty()) {
          TSentryAuthorizable tSentryAuthorizable =
            SentryPolicyServiceClientDefaultImpl.setupSentryAuthorizable(authorizable);
          if (!request.getAuthorizableHierarchy().equals(tSentryAuthorizable)) {
            return false;
          }
        }

        return (request.getRequestorUserName().equalsIgnoreCase(requestorUser) &&
                request.getEntityName().equalsIgnoreCase(entityName));
      }
    });
  }

  private static TListSentryPrivilegesResponse listSentryPrivilegesResponse(Set<TSentryPrivilege> privileges) {
    TListSentryPrivilegesResponse response = new TListSentryPrivilegesResponse();
    response.setStatus(Status.OK());
    response.setPrivileges(privileges);
    return response;
  }
}