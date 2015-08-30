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

public class TestCollectionConfigNoMixing {

  @Test
  public void testBoth() throws Exception {
    CollectionConfigNoMixing validator = new CollectionConfigNoMixing();

    // check no db
    try {
      validator.validate(new PrivilegeValidatorContext("collection=nodb->config=nodb"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException e) {
      ;

    }
    try {
      validator.validate(new PrivilegeValidatorContext("config=nodb->collection=nodb"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException e) {
      ;

    }

    // check with db
    try {
      validator.validate(new PrivilegeValidatorContext("db1","collection=db->config=db"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException e) {
      ;
    }
    try {
      validator.validate(new PrivilegeValidatorContext("db1","config=db->collection=db"));
      Assert.fail("Expected ConfigurationException");
    } catch (ConfigurationException e) {
      ;
    }
  }

  @Test
  public void testCollectionWithoutAction() throws Exception {
    CollectionConfigNoMixing validator = new CollectionConfigNoMixing();
    validator.validate(new PrivilegeValidatorContext("collection=nodb"));
    validator.validate(new PrivilegeValidatorContext("db2","collection=db"));
  }

  @Test
  public void testCollectionWithAction() throws Exception {
    CollectionConfigNoMixing validator = new CollectionConfigNoMixing();
    validator.validate(new PrivilegeValidatorContext(null,"collection=nodb->action=query"));
    validator.validate(new PrivilegeValidatorContext("db2","collection=db->action=update"));
  }

  @Test
  public void testConfigWithoutAction() throws Exception {
    CollectionConfigNoMixing validator = new CollectionConfigNoMixing();
    validator.validate(new PrivilegeValidatorContext("config=nodb"));
    validator.validate(new PrivilegeValidatorContext("db2","config=db"));
  }

  @Test
  public void testConfigWithAction() throws Exception {
    CollectionConfigNoMixing validator = new CollectionConfigNoMixing();
    validator.validate(new PrivilegeValidatorContext(null,"config=nodb->action=*"));
    validator.validate(new PrivilegeValidatorContext("db2","config=db->action=*"));
  }
}
