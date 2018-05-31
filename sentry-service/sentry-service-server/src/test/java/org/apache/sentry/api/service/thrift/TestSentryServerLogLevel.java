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

import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.*;

import java.net.HttpURLConnection;
import java.net.URL;

public class TestSentryServerLogLevel extends SentryServiceIntegrationBase {
  private final String CLASS_NAME = "org.eclipse.jetty.server.handler.ContextHandler";

  @BeforeClass
  public static void setup() throws Exception {
    webServerEnabled = true;
    webSecurity = false;
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

  /**
   * Get the log level for the specified class
   * @param className: Name of class
   * @return
   *        Log level of the class
   */
  private String getLogLevel(String className) {
    Logger logInstance = LogManager.getLogger(className);
    return logInstance.getEffectiveLevel().toString();
  }

  /**
   * Send log level and class name via the HTTP interface and verify that it is set at the loogger.
   * @throws Exception
   */
  @Test
  public void testSetLogLevel() throws Exception {
    final URL url = new URL("http://"+ SERVER_HOST + ":" + webServerPort + "/admin/logLevel?log="
            + CLASS_NAME + "&level=INFO");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    String response = IOUtils.toString(conn.getInputStream());
    Assert.assertTrue(response.contains("INFO"));
    Assert.assertEquals("INFO", getLogLevel(CLASS_NAME));
  }

  /**
   * Send class name via the HTTP interface and verify that it is get at the loogger.
   * @throws Exception
   */
  @Test
  public void testGetLogLevel() throws Exception {
    final URL url = new URL("http://"+ SERVER_HOST + ":" + webServerPort + "/admin/logLevel?log=" + CLASS_NAME);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    String response = IOUtils.toString(conn.getInputStream());
    Assert.assertTrue(response.contains("INFO"));
    Assert.assertEquals("INFO", getLogLevel(CLASS_NAME));
  }

  /**
   * Send class name and invalid log level via the HTTP interface and verify that it returns error response.
   * @throws Exception
   */
  @Test
  public void testInvalidLogLevel() throws Exception {
    final URL url = new URL("http://"+ SERVER_HOST + ":" + webServerPort + "/admin/logLevel?log="
            + CLASS_NAME + "&level=ABCD");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, conn.getResponseCode());
  }
}
