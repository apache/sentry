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

import org.apache.sentry.provider.db.generic.service.thrift.SentryGenericServiceClient;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;

import java.util.Set;

/**
 * The class for admin command to list privileges by role.
 */
public class ListPrivilegesByRoleCmd implements Command {

  private String roleName;
  private String component;
  private String serviceName;
  private TSentryPrivilegeConverter converter;

  public ListPrivilegesByRoleCmd(String roleName, String component, String serviceName,
      TSentryPrivilegeConverter converter) {
    this.roleName = roleName;
    this.component = component;
    this.serviceName = serviceName;
    this.converter = converter;
  }

  @Override
  public void execute(SentryGenericServiceClient client, String requestorName) throws Exception {
    Set<TSentryPrivilege> privileges = client
            .listPrivilegesByRoleName(requestorName, roleName, component, serviceName);
    if (privileges != null) {
      for (TSentryPrivilege privilege : privileges) {
        String privilegeStr = converter.toString(privilege);
        System.out.println(privilegeStr);
      }
    }
  }
}
