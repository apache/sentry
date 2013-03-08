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
package com.cloudera.access.provider.file.shiro;

import java.util.Map;

import org.apache.shiro.authc.SimpleAccount;
import org.apache.shiro.authz.SimpleRole;
import org.apache.shiro.realm.text.IniRealm;
import org.apache.shiro.util.StringUtils;

public class AuthorizationOnlyIniRealm extends IniRealm {

  static final String DUMMY_PASSWORD = "not used";

  public AuthorizationOnlyIniRealm(String resource) {
    super(resource);
  }

  @Override
  protected void processUserDefinitions(Map<String, String> userDefs) {
    /*
     * Overrides the default method since our configuration
     * file format doesn't have a password.
     */
    if (userDefs == null || userDefs.isEmpty()) {
      return;
    }
    for (String username : userDefs.keySet()) {
      String value = userDefs.get(username);
      SimpleAccount account = getUser(username);
      if (account == null) {
        account = new SimpleAccount(username, DUMMY_PASSWORD, getName());
        add(account);
      } else {
        account.setCredentials(DUMMY_PASSWORD);
      }
      String[] roles = StringUtils.split(value);
      if (roles.length > 0) {
        for (int i = 0; i < roles.length; i++) {
          String rolename = roles[i];
          account.addRole(rolename);
          SimpleRole role = getRole(rolename);
          if (role != null) {
            account.addObjectPermissions(role.getPermissions());

          }
        }
      } else {
        account.setRoles(null);
      }
    }
  }

  // left for debugging
  @Override
  protected void processRoleDefinitions(Map<String, String> roleDefs) {
    super.processRoleDefinitions(roleDefs);
  }

}
