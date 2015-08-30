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
package org.apache.sentry.policy.search;

import junit.framework.Assert;

import org.apache.sentry.policy.common.PrivilegeValidatorContext;
import org.apache.shiro.config.ConfigurationException;
import org.junit.Test;

public class TestConfigOnlyAllActionAllowed {

  @Test
  public void testConfigWithBadAction() throws Exception {
    ConfigOnlyAllActionAllowed validator = new ConfigOnlyAllActionAllowed();

    // check no db
    verifyConfException(new PrivilegeValidatorContext("config=nodb->action=query"));
    verifyConfException(new PrivilegeValidatorContext("config=nodb->action=update"));

    // check with db
    verifyConfException(new PrivilegeValidatorContext("db2", "config=nodb->action=query"));
    verifyConfException(new PrivilegeValidatorContext("db2", "config=nodb->action=update"));
  }

  private void verifyConfException(PrivilegeValidatorContext p) throws Exception {
    ConfigOnlyAllActionAllowed validator = new ConfigOnlyAllActionAllowed();
    try {
      validator.validate(p);
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException e) {
      ;
    }
  }

  @Test
  public void testConfigWithoutAction() throws Exception {
    ConfigOnlyAllActionAllowed validator = new ConfigOnlyAllActionAllowed();
    validator.validate(new PrivilegeValidatorContext("config=nodb"));
    validator.validate(new PrivilegeValidatorContext("db2","config=db"));
  }

  @Test
  public void testConfigWithAction() throws Exception {
    ConfigOnlyAllActionAllowed validator = new ConfigOnlyAllActionAllowed();
    validator.validate(new PrivilegeValidatorContext(null,"config=nodb->action=*"));
    validator.validate(new PrivilegeValidatorContext("db2","config=db->action=*"));
  }
}
