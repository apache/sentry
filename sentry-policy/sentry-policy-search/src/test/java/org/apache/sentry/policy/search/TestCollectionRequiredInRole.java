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

public class TestCollectionRequiredInRole {

  @Test
  public void testEmptyRole() throws Exception {
    CollectionRequiredInPrivilege collRequiredInRole = new CollectionRequiredInPrivilege();

    // check no db
    try {
      collRequiredInRole.validate(new PrivilegeValidatorContext("index=index1"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException e) {
      ;
    }

    // check with db
    try {
      collRequiredInRole.validate(new PrivilegeValidatorContext("db1","index=index2"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException e) {
      ;
    }
  }

  @Test
  public void testCollectionWithoutAction() throws Exception {
    CollectionRequiredInPrivilege collRequiredInRole = new CollectionRequiredInPrivilege();
    collRequiredInRole.validate(new PrivilegeValidatorContext("collection=nodb"));
    collRequiredInRole.validate(new PrivilegeValidatorContext("db2","collection=db"));
  }

  @Test
  public void testCollectionWithAction() throws Exception {
    CollectionRequiredInPrivilege collRequiredInRole = new CollectionRequiredInPrivilege();
    collRequiredInRole.validate(new PrivilegeValidatorContext(null,"collection=nodb->action=query"));
    collRequiredInRole.validate(new PrivilegeValidatorContext("db2","collection=db->action=update"));
  }
}
