package org.apache.sentry.tests.e2e.hive;

import java.util.HashMap;
import java.util.Map;

public class StaticUserGroup {
  public static final String
      ADMIN1 = "admin1",
      ADMINGROUP = "admin",
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
