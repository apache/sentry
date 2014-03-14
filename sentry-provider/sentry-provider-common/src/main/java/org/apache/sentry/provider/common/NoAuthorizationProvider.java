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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.sentry.core.common.Action;
import org.apache.sentry.core.common.ActiveRoleSet;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.SentryConfigurationException;
import org.apache.sentry.core.common.Subject;

public class NoAuthorizationProvider implements AuthorizationProvider {
  private GroupMappingService noGroupMappingService = new NoGroupMappingService();

  @Override
  public boolean hasAccess(Subject subject, List<? extends Authorizable> authorizableHierarchy,
      Set<? extends Action> actions, ActiveRoleSet roleSet) {
    return false;
  }

  @Override
  public GroupMappingService getGroupMapping() {
    return noGroupMappingService;
  }

  @Override
  public void validateResource(boolean strictValidation) throws SentryConfigurationException {
    return;
  }

  @Override
  public Set<String> listPrivilegesForSubject(Subject subject)
      throws SentryConfigurationException {
    return new HashSet<String>();
  }

  @Override
  public Set<String> listPrivilegesForGroup(String groupName)
      throws SentryConfigurationException {
    return new HashSet<String>();
  }

  @Override
  public List<String> getLastFailedPrivileges() {
    return new ArrayList<String>();
  }

  @Override
  public void close() {

  }
}
