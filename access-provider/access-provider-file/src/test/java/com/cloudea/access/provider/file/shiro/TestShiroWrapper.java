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
package com.cloudea.access.provider.file.shiro;

import java.util.Arrays;
import java.util.Collection;

import junit.framework.Assert;

import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.access.provider.file.shiro.AuthorizationOnlyIniRealm;
import com.cloudera.access.provider.file.shiro.UsernameToken;

@RunWith(Parameterized.class)
public class TestShiroWrapper {

  private static final String PERM_SERVER1_CUSTOMERS_SELECT = "server=server1:db=customers:table=purchases:select";
  private static final String PERM_SERVER1_CUSTOMERS_INSERT = "server=server1:db=customers:table=purchases:insert";

  private static final String PERM_SERVER1_FUNCTIONS_ADD = "server=server1:functions:add";
  private static final String PERM_SERVER1_ANALYST_ALL = "server=server1:db=analyst:*";
  private static final String PERM_SERVER1_JUNIOR_ANALYST_ALL = "server=server1:db=junior_analyst:*";
  private static final String PERM_SERVER1_JUNIOR_ANALYST_READ = "server=server1:db=junior_analyst:*:select";
  private static final String PERM_ALL_SERVERS_JUNIOR_ANALYST_READ = "*:db=junior_analyst:*:select";

  private static final String PERM_SERVER1_JUNIOR_ANALYST_WRITE = "server=server1:db=junior_analyst:*:insert";
  private static final String PERM_ALL_SERVERS_FUNCTIONS_ALL = "*:functions:*";
  private static final String PERM_ADMIN = "*";

  private static final String USER_ADMIN = "admin";
  private static final String USER_JUNIOR_ANALYST = "junior_analyst";
  private static final String USER_ANALYST = "analyst";
  private static final String USER_MANAGER = "manager";

  private final SecurityManager securityManager;
  private final Subject subject;
  private final String user;
  private final String permission;
  private boolean expected;

  public TestShiroWrapper(String user, String permission, boolean expected) {
    AuthorizationOnlyIniRealm realm = new AuthorizationOnlyIniRealm("classpath:test-authz-provider.ini");
    securityManager = new DefaultSecurityManager(realm);
    ThreadContext.bind(securityManager);
    subject = new Subject.Builder(securityManager).buildSubject();
    subject.login(new UsernameToken(user));
    this.user = user;
    this.permission = permission;
    this.expected = expected;
  }

  @Test
  public void testSubjectPermission() throws Exception {
    Assert.assertEquals(String.format("User = %s, Permission = %s", user, permission),
        expected, subject.isPermitted(permission));
  }

  @Parameters
  public static Collection<Object[]> run() {
    return Arrays.asList(new Object[][] {
        {USER_ADMIN, PERM_ADMIN, true},
        {USER_ADMIN, PERM_SERVER1_CUSTOMERS_SELECT, true},
        {USER_ADMIN, PERM_SERVER1_CUSTOMERS_INSERT, true},
        {USER_ADMIN, PERM_SERVER1_FUNCTIONS_ADD, true},
        {USER_ADMIN, PERM_ALL_SERVERS_FUNCTIONS_ALL, true},
        {USER_ADMIN, PERM_SERVER1_ANALYST_ALL, true},
        {USER_ADMIN, PERM_SERVER1_JUNIOR_ANALYST_ALL, true},
        {USER_ADMIN, PERM_SERVER1_JUNIOR_ANALYST_READ, true},
        {USER_ADMIN, PERM_SERVER1_JUNIOR_ANALYST_WRITE, true},

        {USER_MANAGER, PERM_ADMIN, false},
        {USER_MANAGER, PERM_SERVER1_CUSTOMERS_SELECT, true},
        {USER_MANAGER, PERM_SERVER1_CUSTOMERS_INSERT, false},
        {USER_MANAGER, PERM_SERVER1_FUNCTIONS_ADD, true},
        {USER_MANAGER, PERM_ALL_SERVERS_FUNCTIONS_ALL, true},
        {USER_MANAGER, PERM_SERVER1_ANALYST_ALL, true},
        {USER_MANAGER, PERM_SERVER1_JUNIOR_ANALYST_ALL, true},
        {USER_MANAGER, PERM_SERVER1_JUNIOR_ANALYST_READ, true},
        {USER_MANAGER, PERM_SERVER1_JUNIOR_ANALYST_WRITE, true},

        {USER_ANALYST, PERM_ADMIN, false},
        {USER_ANALYST, PERM_SERVER1_CUSTOMERS_SELECT, true},
        {USER_ANALYST, PERM_SERVER1_CUSTOMERS_INSERT, false},
        {USER_ANALYST, PERM_SERVER1_FUNCTIONS_ADD, true},
        {USER_ANALYST, PERM_ALL_SERVERS_FUNCTIONS_ALL, false},
        {USER_ANALYST, PERM_SERVER1_ANALYST_ALL, true},
        {USER_ANALYST, PERM_SERVER1_JUNIOR_ANALYST_ALL, false},
        {USER_ANALYST, PERM_SERVER1_JUNIOR_ANALYST_READ, true},
        {USER_ANALYST, PERM_SERVER1_JUNIOR_ANALYST_WRITE, false},
        {USER_ANALYST, PERM_ALL_SERVERS_JUNIOR_ANALYST_READ, false},


        {USER_JUNIOR_ANALYST, PERM_ADMIN, false},
        {USER_JUNIOR_ANALYST, PERM_SERVER1_CUSTOMERS_SELECT, false},
        {USER_JUNIOR_ANALYST, PERM_SERVER1_CUSTOMERS_INSERT, false},
        {USER_JUNIOR_ANALYST, PERM_SERVER1_FUNCTIONS_ADD, false},
        {USER_JUNIOR_ANALYST, PERM_ALL_SERVERS_FUNCTIONS_ALL, false},
        {USER_JUNIOR_ANALYST, PERM_SERVER1_ANALYST_ALL, false},
        {USER_JUNIOR_ANALYST, PERM_SERVER1_JUNIOR_ANALYST_ALL, true},
        {USER_JUNIOR_ANALYST, PERM_SERVER1_JUNIOR_ANALYST_READ, true},
        {USER_JUNIOR_ANALYST, PERM_SERVER1_JUNIOR_ANALYST_WRITE, true},
    });
  }
}
