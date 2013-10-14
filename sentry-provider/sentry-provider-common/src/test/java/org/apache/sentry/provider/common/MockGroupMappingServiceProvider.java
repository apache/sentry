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
package org.apache.sentry.provider.common;

import java.util.Collection;
import java.util.List;

import org.apache.sentry.provider.common.GroupMappingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

public class MockGroupMappingServiceProvider implements GroupMappingService {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(MockGroupMappingServiceProvider.class);
  private final Multimap<String, String> userToGroupMap;

  public MockGroupMappingServiceProvider(Multimap<String, String> userToGroupMap) {
    this.userToGroupMap = userToGroupMap;
  }

  @Override
  public List<String> getGroups(String user) {
    Collection<String> groups = userToGroupMap.get(user);
    LOGGER.info("Mapping " + user + " to " + groups);
    return Lists.newArrayList(groups);
  }

}
