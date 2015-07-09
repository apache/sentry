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
package org.apache.sentry.tests.e2e.sqoop;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

public class StaticUserGroupRole {
  public static final String USER_1 = "user1";
  public static final String USER_2 = "user2";
  public static final String USER_3 = "user3";
  public static final String USER_4 = "user4";
  public static final String USER_5 = "user5";

  public static final String GROUP_1 = "group1";
  public static final String GROUP_2 = "group2";
  public static final String GROUP_3 = "group3";
  public static final String GROUP_4 = "group4";
  public static final String GROUP_5 = "group5";

  public static final String ROLE_1 = "role1";
  public static final String ROLE_2 = "role2";
  public static final String ROLE_3 = "role3";
  public static final String ROLE_4 = "role4";
  public static final String ROLE_5 = "role5";

  private static Map<String, Set<String>> userToGroupsMapping =
      new HashMap<String, Set<String>>();

  static {
    userToGroupsMapping.put(USER_1, Sets.newHashSet(GROUP_1));
    userToGroupsMapping.put(USER_2, Sets.newHashSet(GROUP_2));
    userToGroupsMapping.put(USER_3, Sets.newHashSet(GROUP_3));
    userToGroupsMapping.put(USER_4, Sets.newHashSet(GROUP_4));
    userToGroupsMapping.put(USER_5, Sets.newHashSet(GROUP_5));
  }

  public static Set<String> getUsers() {
    return userToGroupsMapping.keySet();
  }

  public static Set<String> getGroups(String user) {
    return userToGroupsMapping.get(user);
  }
}
