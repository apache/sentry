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
package org.apache.sentry.sqoop.authz;

import java.util.List;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;
import org.apache.sqoop.security.AuthenticationProvider;
import org.apache.sqoop.security.authorization.DefaultAuthorizationHandler;

public class SentryAuthorizationHander extends DefaultAuthorizationHandler {
  private static AuthenticationProvider authenticator;

  public static AuthenticationProvider getAuthenticator() {
    if (authenticator == null) {
      throw new RuntimeException("authenticator can't be null");
    }
    return authenticator;
  }
  @Override
  public void doInitialize(AuthenticationProvider authenticationProvider, String serverName)
      throws ClassNotFoundException, IllegalAccessException,
      InstantiationException {
    super.doInitialize(authenticationProvider, serverName);
    authenticator = authenticationProvider;
  }

  @Override
  public void checkPrivileges(MPrincipal principal, List<MPrivilege> privileges)
      throws SqoopException {
    authorizationValidator.checkPrivileges(principal, privileges);
  }

  @Override
  public void createRole(MRole role) throws SqoopException {
    authorizationAccessController.createRole(role);
  }

  @Override
  public void dropRole(MRole role) throws SqoopException {
    authorizationAccessController.dropRole(role);
  }

  @Override
  public List<MRole> getAllRoles() throws SqoopException {
    return authorizationAccessController.getAllRoles();
  }

  @Override
  public List<MPrincipal> getPrincipalsByRole(MRole role) throws SqoopException {
    return authorizationAccessController.getPrincipalsByRole(role);
  }

  @Override
  public List<MPrivilege> getPrivilegesByPrincipal(MPrincipal principal,
      MResource resource) throws SqoopException {
    return authorizationAccessController.getPrivilegesByPrincipal(principal, resource);
  }

  @Override
  public List<MRole> getRolesByPrincipal(MPrincipal principal) throws SqoopException {
    return authorizationAccessController.getRolesByPrincipal(principal);
  }

  @Override
  public void grantPrivileges(List<MPrincipal> principals, List<MPrivilege> privileges)
      throws SqoopException {
    authorizationAccessController.grantPrivileges(principals, privileges);
  }

  @Override
  public void grantRole(List<MPrincipal> principals, List<MRole> roles)
      throws SqoopException {
    authorizationAccessController.grantRole(principals, roles);
  }

  @Override
  public void removeResource(MResource resource) throws SqoopException {
    authorizationAccessController.removeResource(resource);
  }

  @Override
  public void revokePrivileges(List<MPrincipal> principals, List<MPrivilege> privileges)
      throws SqoopException {
    authorizationAccessController.revokePrivileges(principals, privileges);
  }

  @Override
  public void revokeRole(List<MPrincipal> principals, List<MRole> roles)
      throws SqoopException {
    authorizationAccessController.revokeRole(principals, roles);
  }

  @Override
  public void updateResource(MResource srcResource, MResource dstResource)
      throws SqoopException {
    authorizationAccessController.updateResource(srcResource, dstResource);
  }
}
