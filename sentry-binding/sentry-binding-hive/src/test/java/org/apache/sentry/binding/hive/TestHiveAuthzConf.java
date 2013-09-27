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
package org.apache.sentry.binding.hive;

import java.util.Arrays;
import java.util.List;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestHiveAuthzConf {
  private HiveAuthzConf authzConf;
  private HiveAuthzConf authzDepConf;
  private List<AuthzConfVars> currentProps;

  @Before
  public void setUp() {
    authzConf =  new HiveAuthzConf(Resources.getResource("sentry-site.xml"));
    authzDepConf = new HiveAuthzConf(Resources.getResource("sentry-deprecated-site.xml"));
    currentProps = Arrays.asList(new AuthzConfVars[] {
        AuthzConfVars.AUTHZ_PROVIDER, AuthzConfVars.AUTHZ_PROVIDER_RESOURCE,
        AuthzConfVars.AUTHZ_SERVER_NAME, AuthzConfVars.AUTHZ_RESTRICT_DEFAULT_DB,
        AuthzConfVars.SENTRY_TESTING_MODE, AuthzConfVars.AUTHZ_UDF_WHITELIST,
        AuthzConfVars.AUTHZ_ALLOW_HIVE_IMPERSONATION, AuthzConfVars.AUTHZ_ONFAILURE_HOOKS });

  }

  @Test
  public void testConfig() {
    Assert.assertEquals("deprecated",
        authzDepConf.get(AuthzConfVars.AUTHZ_PROVIDER_DEPRECATED.getVar()));
    Assert.assertEquals("org.apache.sentry.provider.file.fooProvider",
        authzConf.get(AuthzConfVars.AUTHZ_PROVIDER.getVar()));
  }

  /**
   * Check that the deprecated property values are used if the current properties
   * are not set.
   */
  @Test
  public void testDeprecatedConfig() {
    for (AuthzConfVars currentVar : currentProps) {
      Assert.assertEquals("deprecated", authzDepConf.get(currentVar.getVar()));
    }
  }

  /**
   * Test that deprecated configs do not override non-deprecated configs
   */
  @Test
  public void testDeprecatedOverride() {
    try {
      for (AuthzConfVars currentVar : currentProps) {
        authzDepConf.set(currentVar.getVar(), "current");
        Assert.assertEquals("current", authzDepConf.get(currentVar.getVar()));
      }
    }
    finally {
      for (AuthzConfVars currentVar : currentProps) {
        authzDepConf.unset(currentVar.getVar());
      }
    }
  }
}
