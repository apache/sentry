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
package org.apache.sentry.api.service.thrift;

import static org.junit.Assert.fail;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.sentry.service.thrift.KerberosConfiguration;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class TestSentryWebServerWithKerberos extends SentryServiceIntegrationBase {

  private static Logger LOG = LoggerFactory.getLogger(TestSentryWebServerWithKerberos.class);

  @BeforeClass
  public static void setup() throws Exception {
    SentryServiceIntegrationBase.webServerEnabled = true;
    SentryServiceIntegrationBase.webSecurity = true;
    SentryServiceIntegrationBase.setup();
  }

  @Override
  @Before
  public void before() throws Exception {
  }

  @Override
  @After
  public void after() {
  }

  @Test
  public void testPing() throws Exception {
    SentryServiceIntegrationBase.clientUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        final URL url = new URL("http://"+ SentryServiceIntegrationBase.SERVER_HOST + ":" + SentryServiceIntegrationBase.webServerPort + "/ping");
        HttpURLConnection conn = new AuthenticatedURL(new KerberosAuthenticator()).
            openConnection(url, new AuthenticatedURL.Token());
        Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        String response = IOUtils.toString(conn.getInputStream());
        Assert.assertEquals("pong\n", response);
      return null;
      }} );
  }

  @Test
  public void testPingWithoutSubject() throws Exception {
    final URL url = new URL("http://"+ SentryServiceIntegrationBase.SERVER_HOST + ":" + SentryServiceIntegrationBase.webServerPort + "/ping");
    try {
      new AuthenticatedURL(new KerberosAuthenticator()).openConnection(url, new AuthenticatedURL.Token());
      fail("Here should fail.");
    } catch (Exception e) {
      boolean isExpectError = e.getMessage().contains("No valid credentials provided");
      Assert.assertTrue("Here should fail by 'No valid credentials provided'," +
          " but the exception is:" + e, isExpectError);
    }
  }

  @Test
  public void testPingUsingHttpURLConnection() throws Exception {
    final URL url = new URL("http://"+ SentryServiceIntegrationBase.SERVER_HOST + ":" + SentryServiceIntegrationBase.webServerPort + "/ping");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
    String errorMessage = IOUtils.toString(conn.getErrorStream());
    Assert.assertTrue(errorMessage.contains("Authentication required"));
  }

  @Test
  public void testPingWithUnauthorizedUser() throws Exception {
    // create an unauthorized User with Kerberos
    String userPrinciple = "user/" + SentryServiceIntegrationBase.SERVER_HOST;
    String userKerberosName = userPrinciple + "@" + SentryServiceIntegrationBase.REALM;
    Subject userSubject = new Subject(false, Sets.newHashSet(
        new KerberosPrincipal(userKerberosName)), new HashSet<Object>(),new HashSet<Object>());
    File userKeytab = new File(SentryServiceIntegrationBase.kdcWorkDir, "user.keytab");
    SentryServiceIntegrationBase.kdc.createPrincipal(userKeytab, userPrinciple);
    LoginContext userLoginContext = new LoginContext("", userSubject, null,
        KerberosConfiguration.createClientConfig(userKerberosName, userKeytab));
    userLoginContext.login();
    Subject.doAs(userLoginContext.getSubject(), new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        final URL url = new URL("http://"+ SentryServiceIntegrationBase.SERVER_HOST + ":" + SentryServiceIntegrationBase.webServerPort + "/ping");
        try {
          new AuthenticatedURL(new KerberosAuthenticator()).openConnection(url, new AuthenticatedURL.Token());
          fail("Here should fail.");
        } catch (AuthenticationException e) {
          String expectedError = "status code: 403";
          if (!e.getMessage().contains(expectedError)) {
            LOG.error("UnexpectedError: " + e.getMessage(), e);
            fail("UnexpectedError: " + e.getMessage());
          }
        }
        return null;
      }
    });
  }

  @Test
  public void testPingWithCaseSensitiveUser() throws Exception {
    // USER1 is present in the list of users who are allowed to connect to sentry web ui.
    String userPrinciple = "user1/" + SentryServiceIntegrationBase.SERVER_HOST;
    String userKerberosName = userPrinciple + "@" + SentryServiceIntegrationBase.REALM;
    Subject userSubject = new Subject(false, Sets.newHashSet(
            new KerberosPrincipal(userKerberosName)), new HashSet<Object>(),new HashSet<Object>());
    File userKeytab = new File(SentryServiceIntegrationBase.kdcWorkDir, "user1.keytab");
    SentryServiceIntegrationBase.kdc.createPrincipal(userKeytab, userPrinciple);
    LoginContext userLoginContext = new LoginContext("", userSubject, null,
            KerberosConfiguration.createClientConfig(userKerberosName, userKeytab));
    userLoginContext.login();
    Subject.doAs(userLoginContext.getSubject(), new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        final URL url = new URL("http://"+ SentryServiceIntegrationBase.SERVER_HOST + ":" + SentryServiceIntegrationBase.webServerPort + "/ping");
        try {
          new AuthenticatedURL(new KerberosAuthenticator()).openConnection(url, new AuthenticatedURL.Token());
          fail("Login with user1 should fail");
        } catch (AuthenticationException e) {
          String expectedError = "status code: 403";
          if (!e.getMessage().contains(expectedError)) {
            LOG.error("UnexpectedError: " + e.getMessage(), e);
            fail("UnexpectedError: " + e.getMessage());
          }
        }
        return null;
      }
    });
  }

  @Test
  public void testTraceIsDisabled() throws Exception {
    final URL url = new URL("http://"+ SentryServiceIntegrationBase.SERVER_HOST + ":" + SentryServiceIntegrationBase.webServerPort);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("TRACE");
    Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());
  }
}
