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
package org.apache.sentry.policy.indexer;

import junit.framework.Assert;

import org.apache.sentry.policy.common.PrivilegeValidatorContext;
import org.apache.shiro.config.ConfigurationException;
import org.junit.Test;

public class TestIndexerRequiredInRole {

  @Test
  public void testEmptyRole() throws Exception {
    IndexerRequiredInPrivilege indexerRequiredInRole = new IndexerRequiredInPrivilege();

    // check no db
    try {
      indexerRequiredInRole.validate(new PrivilegeValidatorContext("index=index1"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException e) {
      ;
    }

    // check with db
    try {
      indexerRequiredInRole.validate(new PrivilegeValidatorContext("db1","index=index2"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException e) {
      ;
    }
  }

  @Test
  public void testIndexerWithoutAction() throws Exception {
    IndexerRequiredInPrivilege indRequiredInRole = new IndexerRequiredInPrivilege();
    indRequiredInRole.validate(new PrivilegeValidatorContext("indexer=nodb"));
    indRequiredInRole.validate(new PrivilegeValidatorContext("db2","indexer=db"));
  }

  @Test
  public void testIndexerWithAction() throws Exception {
    IndexerRequiredInPrivilege indRequiredInRole = new IndexerRequiredInPrivilege();
    indRequiredInRole.validate(new PrivilegeValidatorContext(null,"indexer=nodb->action=read"));
    indRequiredInRole.validate(new PrivilegeValidatorContext("db2","indexer=db->action=write"));
  }
}
