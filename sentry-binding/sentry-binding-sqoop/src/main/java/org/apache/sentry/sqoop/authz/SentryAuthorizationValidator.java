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

import org.apache.sentry.core.common.Subject;
import org.apache.sentry.sqoop.PrincipalDesc;
import org.apache.sentry.sqoop.PrincipalDesc.PrincipalType;
import org.apache.sentry.sqoop.SentrySqoopError;
import org.apache.sentry.sqoop.binding.SqoopAuthBinding;
import org.apache.sentry.sqoop.binding.SqoopAuthBindingSingleton;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MPrincipal;
import org.apache.sqoop.model.MPrivilege;
import org.apache.sqoop.security.AuthorizationValidator;
import org.apache.sqoop.security.SecurityError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SentryAuthorizationValidator extends AuthorizationValidator {
  private static final Logger LOG = LoggerFactory.getLogger(SentryAuthorizationValidator.class);
  private final SqoopAuthBinding binding;

  public SentryAuthorizationValidator() throws Exception {
    this.binding = SqoopAuthBindingSingleton.getInstance().getAuthBinding();
  }

  @Override
  public void checkPrivileges(MPrincipal principal, List<MPrivilege> privileges) throws SqoopException {
    if ((privileges == null) || privileges.isEmpty()) {
      return;
    }
    PrincipalDesc principalDesc = new PrincipalDesc(principal.getName(), principal.getType());
    if (principalDesc.getType() != PrincipalType.USER) {
      throw new SqoopException(SecurityError.AUTH_0014,SentrySqoopError.AUTHORIZE_CHECK_NOT_SUPPORT_FOR_PRINCIPAL);
    }
    for (MPrivilege privilege : privileges) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to authorize check on privilege : " + privilege +
            " for principal: " + principal);
      }
      if (!binding.authorize(new Subject(principalDesc.getName()), privilege)) {
        throw new SqoopException(SecurityError.AUTH_0014, "User " + principalDesc.getName() +
            " does not have privileges for : " + privilege.toString());
      }
    }
  }
}
