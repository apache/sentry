package org.apache.sentry.core.search;
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

import junit.framework.Assert;

import org.apache.sentry.core.model.search.Config;
import org.junit.Test;

public class TestConfig {

  @Test
  public void testSimple() {
    String name = "simple";
    Config simple = new Config(name);
    Assert.assertEquals(simple.getName(), name);
  }

  @Test
  public void testConfigAuthzType() {
    Config config1 = new Config("config1");
    Config config2 = new Config("config2");
    Assert.assertEquals(config1.getAuthzType(), config2.getAuthzType());
    Assert.assertEquals(config1.getTypeName(), config2.getTypeName());
  }

  // just test it doesn't throw NPE
  @Test
  public void testNullConfig() {
    Config nullConfig = new Config(null);
    nullConfig.getName();
    nullConfig.toString();
    nullConfig.getAuthzType();
    nullConfig.getTypeName();
  }
}
