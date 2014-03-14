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

package org.apache.sentry.service.thrift;
import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.concurrent.TimeoutException;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.sentry.provider.db.service.thrift.SentryPolicyServiceClient;
import org.apache.sentry.service.thrift.ServiceConstants.ClientConfig;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public abstract class SentryServiceIntegrationBase extends KerberosSecurityTestcase {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentryServiceIntegrationBase.class);

  static {
    if (System.getProperty("sun.security.krb5.debug", "").trim().isEmpty()) {
      System.setProperty("sun.security.krb5.debug", String.valueOf("true"));
    }
  }

  protected static final String SERVER_HOST = "localhost";
  protected static final String REALM = "EXAMPLE.COM";
  protected static final String SERVER_PRINCIPAL = "sentry/" + SERVER_HOST;
  protected static final String SERVER_KERBEROS_NAME = "sentry/" + SERVER_HOST + "@" + REALM;
  protected static final String CLIENT_PRINCIPAL = "hive/" + SERVER_HOST;
  protected static final String CLIENT_KERBEROS_NAME = "hive/" + SERVER_HOST + "@" + REALM;

  protected SentryService server;
  protected SentryPolicyServiceClient client;
  protected MiniKdc kdc;
  protected File kdcWorkDir;
  protected File dbDir;
  protected File serverKeytab;
  protected File clientKeytab;
  protected Subject clientSubject;
  protected LoginContext clientLoginContext;
  protected final Configuration conf = new Configuration(false);

  @Before
  public void setup() throws Exception {
    beforeSetup();
    setupConf();
    startSentryService();
    connectToSentryService();
    afterSetup();
  }

  public void startSentryService() throws Exception {
    server.start();
    final long start = System.currentTimeMillis();
    while(!server.isRunning()) {
      Thread.sleep(1000);
      if(System.currentTimeMillis() - start > 60000L) {
        throw new TimeoutException("Server did not start after 60 seconds");
      }
    }
  }

  public void setupConf() throws Exception {
    kdc = getKdc();
    kdcWorkDir = getWorkDir();
    serverKeytab = new File(kdcWorkDir, "server.keytab");
    clientKeytab = new File(kdcWorkDir, "client.keytab");
    kdc.createPrincipal(serverKeytab, SERVER_PRINCIPAL);
    kdc.createPrincipal(clientKeytab, CLIENT_PRINCIPAL);

    conf.set(ServerConfig.PRINCIPAL, SERVER_KERBEROS_NAME);
    conf.set(ServerConfig.KEY_TAB, serverKeytab.getPath());
    conf.set(ServerConfig.RPC_ADDRESS, SERVER_HOST);
    conf.set(ServerConfig.RPC_PORT, String.valueOf(0));
    conf.set(ServerConfig.ALLOW_CONNECT, CLIENT_KERBEROS_NAME);
    dbDir = new File(Files.createTempDir(), "sentry_policy_db");
    conf.set(ServerConfig.SENTRY_STORE_JDBC_URL,
        "jdbc:derby:;databaseName=" + dbDir.getPath() + ";create=true");
    server = new SentryServiceFactory().create(conf);
    conf.set(ClientConfig.SERVER_RPC_ADDRESS, server.getAddress().getHostString());
    conf.set(ClientConfig.SERVER_RPC_PORT, String.valueOf(server.getAddress().getPort()));
  }

  public void connectToSentryService() throws Exception {
    // The client should already be logged in when running in hive/impala/solr
    // therefore we must manually login in the integration tests
    clientSubject = new Subject(false, Sets.newHashSet(
                                  new KerberosPrincipal(CLIENT_KERBEROS_NAME)), new HashSet<Object>(),
                                new HashSet<Object>());
    clientLoginContext = new LoginContext("", clientSubject, null,
                                          KerberosConfiguration.createClientConfig(CLIENT_KERBEROS_NAME, clientKeytab));
    clientLoginContext.login();
    clientSubject = clientLoginContext.getSubject();
    client = Subject.doAs(clientSubject, new PrivilegedExceptionAction<SentryPolicyServiceClient>() {
      @Override
      public SentryPolicyServiceClient run() throws Exception {
        return new SentryServiceClientFactory().create(conf);
      }
    });
  }

  @After
  public void tearDown() throws Exception {
    beforeTeardown();
    if(client != null) {
      client.close();
    }
    if(clientLoginContext != null) {
      try {
        clientLoginContext.logout();
      } catch (Exception e) {
        LOGGER.warn("Error logging client out", e);
      }
    }
    if(server != null) {
      server.stop();
    }
    if (dbDir != null) {
      FileUtils.deleteQuietly(dbDir);
    }
    afterTeardown();
  }

  public void beforeSetup() throws Exception {

  }
  public void afterSetup() throws Exception {

  }
  public void beforeTeardown() throws Exception {

  }
  public void afterTeardown() throws Exception {

  }
  protected static void assertOK(TSentryResponseStatus resp) {
    assertStatus(Status.OK, resp);
  }

  protected static void assertStatus(Status status, TSentryResponseStatus resp) {
    if (resp.getValue() !=  status.getCode()) {
      String message = "Expected: " + status + ", Response: " + Status.fromCode(resp.getValue())
          + ", Code: " + resp.getValue() + ", Message: " + resp.getMessage();
      String stackTrace = Strings.nullToEmpty(resp.getStack()).trim();
      if (!stackTrace.isEmpty()) {
        message += ", StackTrace: " + stackTrace;
      }
      Assert.fail(message);
    }
  }
}
