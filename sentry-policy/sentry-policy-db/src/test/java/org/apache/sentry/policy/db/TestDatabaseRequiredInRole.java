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
package org.apache.sentry.policy.db;

import junit.framework.Assert;

import org.apache.sentry.policy.common.PrivilegeValidatorContext;
import org.apache.shiro.config.ConfigurationException;
import org.junit.Test;

public class TestDatabaseRequiredInRole {

  @Test
  public void testURIInPerDbPolicyFile() throws Exception {
    DatabaseRequiredInPrivilege dbRequiredInRole = new DatabaseRequiredInPrivilege();
    System.setProperty("sentry.allow.uri.db.policyfile", "true");
    dbRequiredInRole.validate(new PrivilegeValidatorContext("db1",
      "server=server1->URI=file:///user/db/warehouse/tab1"));
    System.setProperty("sentry.allow.uri.db.policyfile", "false");
  }

  @Test
  public void testURIWithDBInPerDbPolicyFile() throws Exception {
    DatabaseRequiredInPrivilege dbRequiredInRole = new DatabaseRequiredInPrivilege();
    try {
      dbRequiredInRole.validate(new PrivilegeValidatorContext("db1",
        "server=server1->db=db1->URI=file:///user/db/warehouse/tab1"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException e) {
      ;
    }
  }
}
