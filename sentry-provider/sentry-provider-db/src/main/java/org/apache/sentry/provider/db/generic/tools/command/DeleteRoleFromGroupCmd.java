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

import com.google.common.collect.Sets;
import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.tools.SentryShellCommon;

import java.util.Set;

/**
 * Command for deleting groups from a role.
 */
public class DeleteRoleFromGroupCmd implements Command {

  private String roleName;
  private String groups;
  private String component;

  public DeleteRoleFromGroupCmd(String roleName, String groups, String component) {
    this.groups = groups;
    this.roleName = roleName;
    this.component = component;
  }

  @Override
  public void execute(SentryGenericServiceClient client, String requestorName) throws Exception {
    Set<String> groupSet = Sets.newHashSet(groups.split(SentryShellCommon.GROUP_SPLIT_CHAR));
    client.deleteRoleToGroups(requestorName, roleName, component, groupSet);
  }
}
