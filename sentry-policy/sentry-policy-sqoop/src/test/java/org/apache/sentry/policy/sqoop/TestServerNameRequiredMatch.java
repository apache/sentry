/*
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sentry.policy.sqoop;

import junit.framework.Assert;

import org.apache.sentry.policy.common.PrivilegeValidatorContext;
import org.apache.shiro.config.ConfigurationException;
import org.junit.Test;

public class TestServerNameRequiredMatch {
  @Test
  public void testWithoutServerName() {
    ServerNameRequiredMatch serverNameMatch = new ServerNameRequiredMatch("server1");
    try {
      serverNameMatch.validate(new PrivilegeValidatorContext("connector=c1->action=read"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException ex) {
    }
  }
  @Test
  public void testServerNameNotMatch() throws Exception {
    ServerNameRequiredMatch serverNameMatch = new ServerNameRequiredMatch("server1");
    try {
      serverNameMatch.validate(new PrivilegeValidatorContext("server=server2->connector=c1->action=read"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException ex) {
    }
  }
  @Test
  public void testServerNameMatch() throws Exception {
    ServerNameRequiredMatch serverNameMatch = new ServerNameRequiredMatch("server1");
    try {
      serverNameMatch.validate(new PrivilegeValidatorContext("server=server1->connector=c1->action=read"));
    } catch (ConfigurationException ex) {
      Assert.fail("Not expected ConfigurationException");
    }
  }

}
