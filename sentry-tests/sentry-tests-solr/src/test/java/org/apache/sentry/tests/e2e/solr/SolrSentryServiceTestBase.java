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

import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.model.solr.SolrConstants;
import org.apache.sentry.core.model.solr.SolrModelAuthorizable;
import org.apache.sentry.api.generic.thrift.SentryGenericServiceClient;
import org.apache.sentry.api.generic.thrift.TAuthorizable;
import org.apache.sentry.api.generic.thrift.TSentryGrantOption;
import org.apache.sentry.api.generic.thrift.TSentryPrivilege;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrSentryServiceTestBase extends AbstractSolrSentryTestCase {
  private static final Logger log = LoggerFactory.getLogger(SolrSentryServiceTestBase.class);
  protected static TestSentryServer sentrySvc;
  protected static SentryGenericServiceClient sentryClient;
  protected static UncaughtExceptionHandler orgExceptionHandler;

  @BeforeClass
  public static void setupClass() throws Exception {
    Path testDataPath = createTempDir("solr-integration-db-");

    try {
      sentrySvc = new TestSentryServer(testDataPath, getUserGroupMappings());
      sentrySvc.startSentryService();
      sentryClient = sentrySvc.connectToSentryService();
      log.info("Successfully started Sentry service");
    } catch (Exception ex) {
      log.error ("Unexpected exception while starting Sentry service", ex);
      throw ex;
    }

    for (int i = 0; i < 4; i++) {
      sentryClient.createRole(TestSentryServer.ADMIN_USER, "role"+i, COMPONENT_SOLR);
      sentryClient.grantRoleToGroups(TestSentryServer.ADMIN_USER, "role"+i, COMPONENT_SOLR, Collections.singleton("group"+i));
    }

    log.info("Successfully created roles in Sentry service");

    sentryClient.createRole(TestSentryServer.ADMIN_USER, ADMIN_ROLE, COMPONENT_SOLR);
    sentryClient.grantRoleToGroups(TestSentryServer.ADMIN_USER, ADMIN_ROLE, COMPONENT_SOLR,
        Collections.singleton(TestSentryServer.ADMIN_GROUP));
    grantAdminPrivileges(TestSentryServer.ADMIN_USER, ADMIN_ROLE, SolrConstants.ALL, SolrConstants.ALL);

    log.info("Successfully granted admin privileges to " + ADMIN_ROLE);


    System.setProperty(SENTRY_SITE_LOC_SYSPROP, sentrySvc.getSentrySitePath().toString());

    // set the solr for the loginUser and belongs to solr group
    // Note - Solr/Sentry unit tests don't use Hadoop authentication framework. Hence the
    // UserGroupInformation is not available when the request is being processed by the Solr server.
    // The document level security search component requires this UserGroupInformation while querying
    // the roles associated with the user. Please refer to implementation of
    // SentryGenericProviderBackend#getRoles(...) method. Hence this is a workaround to satisfy this requirement.
    UserGroupInformation.setLoginUser(UserGroupInformation.createUserForTesting("solr", new String[]{"solr"}));

    try {
      configureCluster(NUM_SERVERS)
        .withSecurityJson(TEST_PATH().resolve("security").resolve("security.json"))
        .addConfig("cloud-minimal", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .addConfig("cloud-managed", TEST_PATH().resolve("configsets").resolve("cloud-managed").resolve("conf"))
        .addConfig("cloud-minimal_doc_level_security", TEST_PATH().resolve("configsets")
                      .resolve("cloud-minimal_doc_level_security").resolve("conf"))
        .addConfig("cloud-minimal_subset_match", TEST_PATH().resolve("configsets")
                     .resolve("cloud-minimal_subset_match").resolve("conf"))
        .addConfig("cloud-minimal_subset_match_missing_false", TEST_PATH().resolve("configsets")
              .resolve("cloud-minimal_subset_match_missing_false").resolve("conf"))
        .configure();
      log.info("Successfully started Solr service");

    } catch (Exception ex) {
      log.error ("Unexpected exception while starting SolrCloud", ex);
      throw ex;
    }

    log.info("Successfully setup Solr with Sentry service");
  }

  @AfterClass
  public static void cleanUpResources() throws Exception {
    if (sentryClient != null) {
      sentryClient.close();
      sentryClient = null;
    }
    if (sentrySvc != null) {
      sentrySvc.close();
      sentrySvc = null;
    }
    System.clearProperty(SENTRY_SITE_LOC_SYSPROP);
  }

  public static void grantAdminPrivileges(String requestor, String roleName, String entityName, String action)
      throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Admin.name(), entityName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.grantPrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  public static void revokeAdminPrivileges(String requestor, String roleName, String entityName, String action)
      throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Admin.name(), entityName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.revokePrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  public static void grantCollectionPrivileges(String requestor, String roleName,
      String collectionName, String action) throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Collection.name(), collectionName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.grantPrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  public static void revokeCollectionPrivileges(String requestor, String roleName,
      String collectionName, String action) throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Collection.name(), collectionName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.revokePrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  public static void grantConfigPrivileges(String requestor, String roleName, String configName,
      String action) throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Config.name(), configName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.grantPrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  public static void revokeConfigPrivileges(String requestor, String roleName, String configName,
      String action) throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Config.name(), configName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.revokePrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  public static void grantSchemaPrivileges(String requestor, String roleName, String schemaName,
      String action) throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Schema.name(), schemaName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.grantPrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  public static void revokeSchemaPrivileges(String requestor, String roleName, String schemaName,
      String action) throws SentryUserException {
    List<TAuthorizable> auths = new ArrayList<>(1);
    auths.add(new TAuthorizable(SolrModelAuthorizable.AuthorizableType.Schema.name(), schemaName));

    TSentryPrivilege privilege = new TSentryPrivilege(COMPONENT_SOLR, SERVICE_NAME, auths, action);
    privilege.setGrantOption(TSentryGrantOption.TRUE);
    sentryClient.revokePrivilege(requestor, roleName, COMPONENT_SOLR, privilege);
  }

  protected static Map<String, Set<String>> getUserGroupMappings() {
    Map<String, Set<String>> result = new HashMap<>();
    result.put("user0", Collections.singleton("group0"));
    result.put("user1", Collections.singleton("group1"));
    result.put("user2", Collections.singleton("group2"));
    result.put("user3", Collections.singleton("group3"));
    result.put(TestSentryServer.ADMIN_USER, Collections.singleton(TestSentryServer.ADMIN_GROUP));
    result.put("solr", Collections.singleton("solr"));
    result.put("junit", Collections.singleton("junit"));
    result.put("doclevel", Collections.singleton("doclevel"));
    result.put("user3", Collections.singleton("group3"));

    result.put("subset_user_012", Sets.newHashSet("subset_group0", "subset_group1", "subset_group2", "subset_nogroup"));
    result.put("subset_user_013", Sets.newHashSet("subset_group0", "subset_group1", "subset_group3", "subset_nogroup"));
    result.put("subset_user_023", Sets.newHashSet("subset_group0", "subset_group2", "subset_group3", "subset_nogroup"));
    result.put("subset_user_123", Sets.newHashSet("subset_group1", "subset_group2", "subset_group3", "subset_nogroup"));
    result.put("subset_user_0", Sets.newHashSet("subset_group0", "subset_nogroup"));
    result.put("subset_user_2", Sets.newHashSet("subset_group2", "subset_nogroup"));
    result.put("subset_user_01", Sets.newHashSet("subset_group0", "subset_group1", "subset_nogroup", "subset_delete"));
    result.put("subset_user_23", Sets.newHashSet("subset_group2", "subset_group3", "subset_nogroup"));
    result.put("subset_user_0123", Sets.newHashSet("subset_group0", "subset_group1", "subset_group2", "subset_group3", "subset_nogroup", "subset_delete"));
    result.put("subset_user_no", Sets.newHashSet("subset_nogroup"));

    return Collections.unmodifiableMap(result);
  }


}
