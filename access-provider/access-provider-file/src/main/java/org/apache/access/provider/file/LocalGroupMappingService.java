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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.shiro.config.Ini;
import org.apache.shiro.config.Ini.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mapping users to groups
 * parse the ini file with section [users] that contains the user names. For each user in that list, there's section that contains the group name for that user
 * If there's no user section or no group section for one of users, then just print a warning and continue.
 * Example -
 * [users]
 * usr1
 * usr2
 *
 * [[usr1]
 * group1
 * group11
 *
 * [usr2]
 * group21
 * group22
 *
 */
public class LocalGroupMappingService implements GroupMappingService {

  private String USERS_SECTION = "users";
  private static final Logger LOGGER = LoggerFactory
      .getLogger(LocalGroupMappingService.class);

  private Map <String, List<String>> groupMap =
      new HashMap <String, List<String>> ();

  public LocalGroupMappingService(String resourcePath) {
    // parse user/group mapping
    parseGroups(resourcePath);
  }

  @Override
  public List<String> getGroups(String user) {
    if (groupMap.containsKey(user)) {
      return groupMap.get(user);
    } else {
      return null;
    }
  }

  private void parseGroups(String resourcePath) {
    Ini ini = Ini.fromResourcePath(resourcePath);
    Section usersSection = ini.getSection(USERS_SECTION);
    if (usersSection == null) {
      LOGGER.warn("No section " + USERS_SECTION + " in the " + resourcePath);
      return;
    }
    for (Entry<String, String> userEntry : usersSection.entrySet()) {
      String userName = userEntry.getKey();
      String groupNames = userEntry.getValue();
      if (userName == null) {
        LOGGER.error("Invalid user name in the " + resourcePath);
        continue;
      }
      if (groupNames == null || groupNames.isEmpty()) {
        LOGGER.warn("No groups available for user " + userName +
            " in the " + resourcePath);
        continue;
      }

      List<String> groupList = Arrays.asList(groupNames.split(","));
      // add the group,userList to the map of groups

      LOGGER.debug("Got mapping User:" + userName + "Groups:" + groupNames);
      groupMap.put(userName, groupList);
    }
  }

}
