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
 * Unless createRequired by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.db.service.thrift;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.apache.sentry.service.thrift.SentryWebMetricParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PrivilegedExceptionAction;

public class TestSentryServiceMetrics extends SentryServiceIntegrationBase {

  @BeforeClass
  public static void setup() throws Exception {
    kerberos = true;
    webServerEnabled = true;
    webSecurity = true;
    setupConf();
    startSentryService();
  }

  //Overriding this method as the tests do not require a client handle
  @Override
  @Before
  public void before() throws Exception {

  }
  /* SENTRY-1319 */
  @Test
  public void testSentryServiceGauges() throws Throwable {
    //More Cases to be added once Sentry HA is implemented

    //Check for gauges with the server handle.
    Assert.assertEquals(Boolean.TRUE,server.getIsActiveGauge().getValue());
  }

  @Test
  public void testMetricsWeb() throws Exception {
    clientUgi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        final URL url = new URL("http://"+ SERVER_HOST + ":" + webServerPort + "/metrics");
        HttpURLConnection conn = new AuthenticatedURL(new KerberosAuthenticator()).
                openConnection(url, new AuthenticatedURL.Token());
        //make sure we are able to access the metrics page
        Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
        String response = IOUtils.toString(conn.getInputStream());
        SentryWebMetricParser mp = new SentryWebMetricParser(response);
        Assert.assertEquals(Boolean.FALSE,mp.isHA());
        Assert.assertEquals(Boolean.TRUE,mp.isActive());
        return null;
      }} );
  }

  //Overriding this method as the client handle does not exist.
  @Override
  @After
  public void after() {

  }
}
