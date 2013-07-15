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

import org.apache.sentry.binding.hive.conf.HiveAuthzConf;
import org.apache.sentry.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestHiveAuthzConf {
  private HiveAuthzConf authzConf;
  private HiveAuthzConf authzDepConf;

  @Before
  public void setUp() {
    authzConf =  new HiveAuthzConf(Resources.getResource("sentry-site.xml"));
    authzDepConf =  new HiveAuthzConf(Resources.getResource("access-site.xml"));
  }

  @Test
  public void testConfig() {
    Assert.assertEquals("org.apache.sentry.provider.file.fooProvider",
        authzDepConf.get(AuthzConfVars.AUTHZ_PROVIDER_DEPRECATED.getVar()));
    Assert.assertEquals("org.apache.sentry.provider.file.fooProvider",
        authzConf.get(AuthzConfVars.AUTHZ_PROVIDER.getVar()));
  }

  @Test
  public void testConfigOverload() {
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), "fooFile");
    Assert.assertEquals("fooFile",
        authzConf.get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar()));
    authzDepConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE_DEPRECATED.getVar(), "fooFile");
    Assert.assertEquals("fooFile",
        authzDepConf.get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE_DEPRECATED.getVar()));
  }

  /**
   * Check the deprecated properties from the config files that doesn't explicitly set it
   */
  @Test
  public void testDeprecatedConfig() {
    Assert.assertEquals("classpath:test-authz-provider.ini",
        authzConf.get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE_DEPRECATED.getVar()));
  }
}
