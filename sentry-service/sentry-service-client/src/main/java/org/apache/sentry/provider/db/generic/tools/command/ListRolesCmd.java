/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.provider.db.generic.tools.command;

import org.apache.commons.lang.StringUtils;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryRole;

import java.util.Set;

/**
 * The class for admin command to list roles.
 */
public class ListRolesCmd implements Command {

  private String groupName;
  private String component;

  public ListRolesCmd(String groupName, String component) {
    this.groupName = groupName;
    this.component = component;
  }

  @Override
  public void execute(SentryGenericServiceClient client, String requestorName) throws Exception {
    Set<TSentryRole> roles;
    if (StringUtils.isEmpty(groupName)) {
      roles = client.listAllRoles(requestorName, component);
    } else {
      roles = client.listRolesByGroupName(requestorName, groupName, component);
    }
    if (roles != null) {
      for (TSentryRole role : roles) {
        System.out.println(role.getRoleName());
      }
    }
  }
}
