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
package org.apache.sentry.api.service.thrift;

import java.util.Map;

/**
 * Wrapper around TListSentryPrivilegesByAuthResponse.
 * <p>
 * Allows getting all the results associated with the response
 * in a single fetch
 */
public class SentryObjectPrivileges {

  private Map<TSentryAuthorizable, TSentryPrivilegeMap> privilegesForRoles;
  private  Map<TSentryAuthorizable, TSentryPrivilegeMap> privilegesForUsers;

  SentryObjectPrivileges(TListSentryPrivilegesByAuthResponse response) {

    privilegesForRoles = response.getPrivilegesMapByAuth();
    privilegesForUsers = response.getPrivilegesMapByAuthForUsers();
  }

  /**
   * Return a map of authorizable to a mapping of roleNames to TSentryPrivileges
   * @return
   */
  public Map<TSentryAuthorizable, TSentryPrivilegeMap> getPrivilegesForRoles() {
    return privilegesForRoles;
  }

  /**
   * Return a map of authorizable to a mapping of userNames to TSentryPrivileges
   * @return
   */
  public Map<TSentryAuthorizable, TSentryPrivilegeMap> getPrivilegesForUsers() {
    return privilegesForUsers;
  }
}
