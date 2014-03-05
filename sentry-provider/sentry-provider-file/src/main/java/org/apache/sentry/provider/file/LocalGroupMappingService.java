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

package org.apache.sentry.provider.file;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.sentry.provider.common.GroupMappingService;
import org.apache.shiro.config.Ini;
import org.apache.shiro.config.Ini.Section;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

/**
 * Mapping users to groups
 * parse the ini file with section [users] that contains the user names.
 * For each user in that list, there's section that contains the group
 * name for that user If there's no user section or no group section for
 * one of users, then just print a warning and continue.
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

  private static final Logger LOGGER = LoggerFactory
      .getLogger(LocalGroupMappingService.class);

  private final Map <String, Set<String>> groupMap =
      new HashMap <String, Set<String>> ();

  public LocalGroupMappingService(Path resourcePath) throws IOException {
    this(new Configuration(), resourcePath);
  }
  @VisibleForTesting
  public LocalGroupMappingService(Configuration configuration, Path resourcePath) throws IOException {
    // parse user/group mapping
    parseGroups(resourcePath.getFileSystem(configuration), resourcePath);
  }

  @Override
  public Set<String> getGroups(String user) {
    if (groupMap.containsKey(user)) {
      return groupMap.get(user);
    } else {
      return Collections.emptySet();
    }
  }

  private void parseGroups(FileSystem fileSystem, Path resourcePath) throws IOException {
    Ini ini = PolicyFiles.loadFromPath(fileSystem, resourcePath);
    Section usersSection = ini.getSection(PolicyFileConstants.USERS);
    if (usersSection == null) {
      LOGGER.warn("No section " + PolicyFileConstants.USERS + " in the " + resourcePath);
      return;
    }
    for (Entry<String, String> userEntry : usersSection.entrySet()) {
      String userName = Strings.nullToEmpty(userEntry.getKey()).trim();
      String groupNames = Strings.nullToEmpty(userEntry.getValue()).trim();
      if (userName.isEmpty()) {
        LOGGER.error("Invalid user name in the " + resourcePath);
        continue;
      }
      if (groupNames.isEmpty()) {
        LOGGER.warn("No groups available for user " + userName +
            " in the " + resourcePath);
        continue;
      }
      Set<String> groupList = Sets.newHashSet(
          PolicyFileConstants.ROLE_SPLITTER.trimResults().split(groupNames));
      LOGGER.debug("Got user mapping: " + userName + ", Groups: " + groupNames);
      groupMap.put(userName, groupList);
    }
  }

}
