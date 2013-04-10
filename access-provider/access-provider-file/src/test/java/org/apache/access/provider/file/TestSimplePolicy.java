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
package org.apache.access.provider.file;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import junit.framework.Assert;

import org.apache.access.core.Authorizable;
import org.apache.access.core.Database;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class TestSimplePolicy {
  private static final String PERM_SERVER1_CUSTOMERS_SELECT = "server=server1:db=customers:table=purchases:action=select";
  private static final String PERM_SERVER1_FUNCTIONS_ALL = "server=server1:functions:action=*";
  private static final String PERM_SERVER1_ANALYST_ALL = "server=server1:db=analyst1";
  private static final String PERM_SERVER1_JUNIOR_ANALYST_ALL = "server=server1:db=jranalyst1";
  private static final String PERM_SERVER1_JUNIOR_ANALYST_READ = "server=server1:db=jranalyst1:table=*:action=select";
  private static final String PERM_SERVER1_OTHER_GROUP_DB_CUSTOMERS_SELECT = "server=server1:db=other_group_db:table=purchases:action=select";

  private static final String PERM_SERVER1_ADMIN = "server=server1:db=*";
  private Policy policyFile;
  private List<Authorizable> authorizables;
  @Before
  public void setup() {
    policyFile = new SimplePolicy("classpath:test-authz-provider.ini");
    authorizables = Lists.newArrayList();
  }

  @Test
  public void testManager() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(
        PERM_SERVER1_CUSTOMERS_SELECT, PERM_SERVER1_ANALYST_ALL,
        PERM_SERVER1_JUNIOR_ANALYST_ALL, PERM_SERVER1_JUNIOR_ANALYST_READ
//        TODO figure out how to handle functions
//        ,PERM_SERVER1_FUNCTIONS_ALL
        ));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policyFile.getPermissions(authorizables, list("manager")).values())
        .toString());
  }

  @Test
  public void testAnalyst() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(
        PERM_SERVER1_CUSTOMERS_SELECT, PERM_SERVER1_ANALYST_ALL,
        PERM_SERVER1_JUNIOR_ANALYST_READ));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policyFile.getPermissions(authorizables, list("analyst")).values())
        .toString());
  }

  @Test
  public void testJuniorAnalyst() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets
        .newHashSet(PERM_SERVER1_JUNIOR_ANALYST_ALL));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policyFile.getPermissions(authorizables, list("jranalyst")).values())
        .toString());
  }

  @Test
  public void testAdmin() throws Exception {
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(PERM_SERVER1_ADMIN));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policyFile.getPermissions(authorizables, list("admin")).values())
        .toString());
  }


  @Test
  public void testOtherGroup() throws Exception {
    authorizables.add(new Database("other_group_db"));
    Set<String> expected = Sets.newTreeSet(Sets.newHashSet(
        PERM_SERVER1_OTHER_GROUP_DB_CUSTOMERS_SELECT));
    Assert.assertEquals(expected.toString(),
        new TreeSet<String>(policyFile.getPermissions(authorizables, list("other_group")).values())
        .toString());
  }

  private static List<String> list(String... values) {
    return Lists.newArrayList(values);
  }
}
