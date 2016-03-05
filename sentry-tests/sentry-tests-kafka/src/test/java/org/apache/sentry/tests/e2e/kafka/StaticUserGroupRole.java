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
package org.apache.sentry.tests.e2e.kafka;

import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StaticUserGroupRole {
  public static final String SUPERUSER = "superuser";
  public static final String USER_1 = "user1";
  public static final String USER_2 = "user2";
  public static final String USER_KAFKA = "kafka";

  public static final String GROUP_0 = "group0";
  public static final String GROUP_1 = "group1";
  public static final String GROUP_2 = "group2";
  public static final String GROUP_KAFKA = "group_kafka";

  public static final String ROLE_0 = "role0";
  public static final String ROLE_1 = "role1";
  public static final String ROLE_2 = "role2";

  private static Map<String, Set<String>> userToGroupsMapping =
      new HashMap<String, Set<String>>();

  static {
    userToGroupsMapping.put(SUPERUSER, Sets.newHashSet(GROUP_0));
    userToGroupsMapping.put(USER_1, Sets.newHashSet(GROUP_1));
    userToGroupsMapping.put(USER_2, Sets.newHashSet(GROUP_2));
    userToGroupsMapping.put(USER_KAFKA, Sets.newHashSet(GROUP_KAFKA));
  }

  public static Set<String> getUsers() {
    return userToGroupsMapping.keySet();
  }

  public static Set<String> getGroups(String user) {
    return userToGroupsMapping.get(user);
  }
}
