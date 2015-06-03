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

import org.apache.log4j.Logger;
import org.apache.sentry.core.common.Subject;
import org.apache.sentry.sqoop.PrincipalDesc;
import org.apache.sentry.sqoop.PrincipalDesc.PrincipalType;
import org.apache.sentry.sqoop.SentrySqoopError;
import org.apache.sentry.sqoop.binding.SqoopAuthBinding;
import org.apache.sentry.sqoop.binding.SqoopAuthBindingSingleton;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.model.MResource;
import org.apache.sqoop.model.MRole;
import org.apache.sqoop.security.AuthorizationAccessController;
import org.apache.sqoop.security.SecurityError;

public class SentryAccessController extends AuthorizationAccessController {
  private static final Logger LOG = Logger.getLogger(SentryAccessController.class);
  private final SqoopAuthBinding binding;

  public SentryAccessController() throws Exception {
    this.binding = SqoopAuthBindingSingleton.getInstance().getAuthBinding();
  }

  private Subject getSubject() {
    return new Subject(SentryAuthorizationHander.getAuthenticator().getUserName());
  }

  @Override
  public void createRole(MRole role) throws SqoopException {
    binding.createRole(getSubject(), role.getName());
  }

  @Override
  public void dropRole(MRole role) throws SqoopException {
    binding.dropRole(getSubject(), role.getName());
  }

  @Override
  public List<MRole> getAllRoles() throws SqoopException {
    return binding.listAllRoles(getSubject());
  }

  @Override
  public List<MPrincipal> getPrincipalsByRole(MRole role) throws SqoopException {
    /**
     * Sentry does not implement this function yet
     */
    throw new SqoopException(SecurityError.AUTH_0014, SentrySqoopError.NOT_IMPLEMENT_YET);
  }

  @Override
  public List<MPrivilege> getPrivilegesByPrincipal(MPrincipal principal,
      MResource resource) throws SqoopException {
    /**
     * Sentry Only supports get privilege by role
     */
    PrincipalDesc principalDesc = PrincipalDesc.fromStr(principal.getName(), principal.getType());
    if (principalDesc.getType() != PrincipalType.ROLE) {
      throw new SqoopException(SecurityError.AUTH_0014,
          SentrySqoopError.SHOW_PRIVILEGE_NOT_SUPPORTED_FOR_PRINCIPAL
              + principalDesc.getType().name());
    }
    return binding.listPrivilegeByRole(getSubject(), principalDesc.getName(), resource);
  }

  @Override
  public List<MRole> getRolesByPrincipal(MPrincipal principal) throws SqoopException {
    /**
     * Sentry Only supports get privilege by role
     */
    PrincipalDesc principalDesc = PrincipalDesc.fromStr(principal.getName(), principal.getType());
    if (principalDesc.getType() != PrincipalType.GROUP) {
      throw new SqoopException(SecurityError.AUTH_0014,
          SentrySqoopError.SHOW_GRANT_NOT_SUPPORTED_FOR_PRINCIPAL
              + principalDesc.getType().name());
    }
    return binding.listRolesByGroup(getSubject(), principalDesc.getName());
  }

  @Override
  public void grantPrivileges(List<MPrincipal> principals, List<MPrivilege> privileges)
      throws SqoopException {
    for (MPrincipal principal : principals) {
      PrincipalDesc principalDesc = PrincipalDesc.fromStr(principal.getName(), principal.getType());
      if (principalDesc.getType() != PrincipalType.ROLE) {
        throw new SqoopException(SecurityError.AUTH_0014,
            SentrySqoopError.GRANT_REVOKE_PRIVILEGE_NOT_SUPPORT_FOR_PRINCIPAL
                + principalDesc.getType().name());
      }

      for (MPrivilege privilege : privileges) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Going to grant privilege : " + privilege +
              " to principal: " + principal);
        }
        binding.grantPrivilege(getSubject(), principal.getName(), privilege);
      }
    }
  }

  @Override
  public void grantRole(List<MPrincipal> principals, List<MRole> roles)
      throws SqoopException {
    for (MPrincipal principal : principals) {
      PrincipalDesc principalDesc = PrincipalDesc.fromStr(principal.getName(), principal.getType());
      if (principalDesc.getType() != PrincipalType.GROUP) {
        throw new SqoopException(SecurityError.AUTH_0014,
            SentrySqoopError.GRANT_REVOKE_ROLE_NOT_SUPPORT_FOR_PRINCIPAL
                + principalDesc.getType().name());
      }
      for (MRole role : roles) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Going to grant role : " + role.getName() +
              " to principal: " + principal);
        }
        binding.grantGroupToRole(getSubject(), principal.getName(), role);
      }
    }
  }

  @Override
  public void removeResource(MResource resource) throws SqoopException {
    binding.dropPrivilege(resource);
  }

  @Override
  public void revokePrivileges(List<MPrincipal> principals, List<MPrivilege> privileges)
      throws SqoopException {
    for (MPrincipal principal : principals) {
      PrincipalDesc principalDesc = PrincipalDesc.fromStr(principal.getName(), principal.getType());
      if (principalDesc.getType() != PrincipalType.ROLE) {
        throw new SqoopException(SecurityError.AUTH_0014,
            SentrySqoopError.GRANT_REVOKE_PRIVILEGE_NOT_SUPPORT_FOR_PRINCIPAL
                + principalDesc.getType().name());
      }

      for (MPrivilege privilege : privileges) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Going to revoke privilege : " + privilege +
              " from principal: " + principal);
        }
        binding.revokePrivilege(getSubject(), principal.getName(), privilege);
      }
    }
  }

  @Override
  public void revokeRole(List<MPrincipal> principals, List<MRole> roles)
      throws SqoopException {
    for (MPrincipal principal : principals) {
      PrincipalDesc principalDesc = PrincipalDesc.fromStr(principal.getName(), principal.getType());
      if (principalDesc.getType() != PrincipalType.GROUP) {
        throw new SqoopException(SecurityError.AUTH_0014,
            SentrySqoopError.GRANT_REVOKE_ROLE_NOT_SUPPORT_FOR_PRINCIPAL
                + principalDesc.getType().name());
      }
      for (MRole role : roles) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Going to revoke role : " + role.getName() +
              " from principal: " + principal);
        }
        binding.revokeGroupfromRole(getSubject(), principal.getName(), role);
      }
    }
  }

  @Override
  public void updateResource(MResource srcResource, MResource dstResource)
      throws SqoopException {
    binding.renamePrivilege(getSubject(), srcResource, dstResource);
  }
}
