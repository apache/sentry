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
package org.apache.access.binding.hive;

import org.apache.access.binding.hive.conf.HiveAuthzConf;
import org.apache.access.binding.hive.conf.HiveAuthzConf.AuthzConfVars;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Resources;

public class TestHiveAuthzConf {
  private HiveAuthzConf authzConf;

  @Before
  public void setUp() {
    authzConf =  new HiveAuthzConf(Resources.getResource("access-site.xml"));
  }

  @Test
  public void testConfig() {
    Assert.assertEquals("org.apache.access.provider.file.fooProvider",
        authzConf.get(AuthzConfVars.AUTHZ_PROVIDER.getVar()));
  }

  @Test
  public void testConfigOverload() {
    authzConf.set(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar(), "fooFile");
    Assert.assertEquals("fooFile",
        authzConf.get(AuthzConfVars.AUTHZ_PROVIDER_RESOURCE.getVar()));
  }
}
