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
package org.apache.sentry.provider.db.service.thrift;

import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSentryWebServerWithoutSecurity extends SentryServiceIntegrationBase {

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

  @Test
  public void testPing() throws Exception {
    final URL url = new URL("http://"+ SERVER_HOST + ":" + webServerPort + "/ping");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    String response = IOUtils.toString(conn.getInputStream());
    Assert.assertEquals("pong\n", response);
  }

  @Test
  public void testConf() throws Exception {
    // test bad format
    final URL url = new URL("http://" + SERVER_HOST + ":" + webServerPort + "/conf?"
        + ConfServlet.FORMAT_PARAM + "=badformat");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, conn.getResponseCode());

    // test json format
    final URL url1 = new URL("http://" + SERVER_HOST + ":" + webServerPort + "/conf?"
        + ConfServlet.FORMAT_PARAM +"=" +  ConfServlet.FORMAT_JSON);
    conn = (HttpURLConnection) url1.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());

    // test xml format
    final URL url2 = new URL("http://" + SERVER_HOST + ":" + webServerPort + "/conf?"
        + ConfServlet.FORMAT_PARAM +"=" + ConfServlet.FORMAT_XML);
    conn = (HttpURLConnection) url2.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    String xmlResponse = IOUtils.toString(conn.getInputStream());

    // test default is xml format
    final URL url3 = new URL("http://" + SERVER_HOST + ":" + webServerPort + "/conf");
    conn = (HttpURLConnection) url3.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    String defaultResponse = IOUtils.toString(conn.getInputStream());
    Assert.assertEquals(xmlResponse, defaultResponse);
  }

  @Test
  public void testTraceIsDisabled() throws Exception {
    final URL url = new URL("http://"+ SERVER_HOST + ":" + webServerPort);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("TRACE");
    Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());
  }
}
