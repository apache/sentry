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
package org.apache.sentry.sqoop;

import java.util.Arrays;
import java.util.List;

import org.apache.sentry.sqoop.conf.SqoopAuthConf;
import org.apache.sentry.sqoop.conf.SqoopAuthConf.AuthzConfVars;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestSqoopAuthConf {
  private static SqoopAuthConf authAllConf;
  private static SqoopAuthConf authNoConf;
  private static List<AuthzConfVars> currentProps;

  @BeforeClass
  public static void setup() throws Exception {
    authAllConf = new SqoopAuthConf(Resources.getResource("sentry-site.xml"));
    authNoConf = new SqoopAuthConf(Resources.getResource("no-configure-sentry-site.xml"));
    currentProps = Arrays.asList(new AuthzConfVars[]{
        AuthzConfVars.AUTHZ_PROVIDER, AuthzConfVars.AUTHZ_PROVIDER_BACKEND,
        AuthzConfVars.AUTHZ_POLICY_ENGINE, AuthzConfVars.AUTHZ_PROVIDER_RESOURCE
    });
  }

  @Test
  public void testPropertiesHaveConfigured() {
    Assert.assertEquals("org.apache.sentry.provider.common.HadoopGroupResourceAuthorizationProvider",
            authAllConf.get(AuthzConfVars.AUTHZ_PROVIDER.getVar()));
    Assert.assertEquals("classpath:test-authz-provider.ini",
            authAllConf.get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar()));
    Assert.assertEquals("org.apache.sentry.policy.sqoop.SimpleSqoopPolicyEngine",
            authAllConf.get(AuthzConfVars.AUTHZ_POLICY_ENGINE.getVar()));
    Assert.assertEquals("true", authAllConf.get(AuthzConfVars.AUTHZ_TESTING_MODE.getVar()));
  }

  @Test
  public void testPropertiesNoConfigured() {
    for (AuthzConfVars currentVar : currentProps) {
      Assert.assertEquals(currentVar.getDefault(), authNoConf.get(currentVar.getVar()));
    }
  }
}
