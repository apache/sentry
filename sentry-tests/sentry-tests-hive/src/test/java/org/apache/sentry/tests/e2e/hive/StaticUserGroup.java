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
package org.apache.sentry.tests.e2e.hive;

import java.util.HashMap;
import java.util.Map;

public class StaticUserGroup {
  public static String ADMIN1,ADMINGROUP;
  public static final String
      USER1_1 = "user1_1",
      USER1_2 = "user1_2",
      USER2_1 = "user2_1",
      USER3_1 = "user3_1",
      USER4_1 = "user4_1",
      USERGROUP1 = "user_group1",
      USERGROUP2 = "user_group2",
      USERGROUP3 = "user_group3",
      USERGROUP4 = "user_group4";
  private static final Map<String, String> staticMapping;

  static {

    ADMIN1 = System.getProperty("sentry.e2etest.admin.user", "admin1");
    ADMINGROUP = System.getProperty("sentry.e2etest.admin.group", "admin");
    staticMapping = new HashMap<String, String>();
    staticMapping.put(ADMIN1, ADMINGROUP);
    staticMapping.put(USER1_1, USERGROUP1);
    staticMapping.put(USER1_2, USERGROUP1);
    staticMapping.put(USER2_1, USERGROUP2);
    staticMapping.put(USER3_1, USERGROUP3);
    staticMapping.put(USER4_1, USERGROUP4);
  }

  public static Map<String, String> getStaticMapping(){
    return staticMapping;
  }

}
