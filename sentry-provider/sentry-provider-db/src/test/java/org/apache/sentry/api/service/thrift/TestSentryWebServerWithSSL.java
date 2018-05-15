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

import com.google.common.io.Resources;
import java.net.HttpURLConnection;
import org.apache.commons.io.IOUtils;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.*;

import javax.net.ssl.HttpsURLConnection;
import java.net.URL;
import java.util.Properties;

/**
 * Test sentry web server when ssl is enabled.
 */
public class TestSentryWebServerWithSSL extends SentryServiceIntegrationBase {
  @BeforeClass
  public static void setup() throws Exception {
    webServerEnabled = true;
    webSecurity = false;
    useSSL = true;
    SentryServiceIntegrationBase.setup();
  }

  @Test
  public void testPing() throws Exception {
    final URL url = new URL("https://"+ SERVER_HOST + ":" + webServerPort + "/ping");
    Properties systemProps = System.getProperties();
    systemProps.put( "javax.net.ssl.trustStore", Resources.getResource("cacerts.jks").getPath());
    System.setProperties(systemProps);
    HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
    Assert.assertEquals(HttpsURLConnection.HTTP_OK, conn.getResponseCode());
    String response = IOUtils.toString(conn.getInputStream());
    Assert.assertEquals("pong\n", response);
  }

  @Test
  public void testTraceIsDisabled() throws Exception {
    final URL url = new URL("https://"+ SERVER_HOST + ":" + webServerPort);
    Properties systemProps = System.getProperties();
    systemProps.put( "javax.net.ssl.trustStore", Resources.getResource("cacerts.jks").getPath());
    System.setProperties(systemProps);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("TRACE");
    Assert.assertEquals(HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());
  }
}
