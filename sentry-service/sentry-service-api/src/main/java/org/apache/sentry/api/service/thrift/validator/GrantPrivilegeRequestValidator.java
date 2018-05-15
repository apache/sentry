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

package org.apache.sentry.api.service.thrift.validator;

import java.util.Set;

import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleGrantPrivilegeRequest;
import org.apache.sentry.api.service.thrift.TSentryGrantOption;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;

/**
 * Check's for mandatory fields in the privileges and
 * checks to see if the UNSET option is present.
 */
public final class GrantPrivilegeRequestValidator {
  private GrantPrivilegeRequestValidator() {
  }

  /**
   * Validates privileges in input request by making sure mandatory fields like
   * server name and action in the privileges are not empty and see all the values in the
   * request are valid.
   *
   * @param request to be validated.
   * @throws SentryInvalidInputException If all the mandatory fields in the privileges are
   *                                     not present [OR] invalid fields a provided in request.
   */
  public static void validate(TAlterSentryRoleGrantPrivilegeRequest request)
    throws SentryInvalidInputException {
    if (request.isSetPrivileges() && (!request.getPrivileges().isEmpty())) {
      checkForMandatoryFieldsInPrivileges(request.getPrivileges());
      validateGrantOptionInprivileges(request.getPrivileges());
    }
  }

  /**
   * Checks for mandatory fields "serverName" and "action" in all the privileges
   * in the set are not empty.
   *
   * @param privileges Set of <code>TSentryPrivileges</code> to be inspected
   * @throws SentryInvalidInputException If all the mandatory fields in the privileges are
   *                                     not present
   */
  static void checkForMandatoryFieldsInPrivileges(Set<TSentryPrivilege> privileges)
    throws SentryInvalidInputException {
    for (TSentryPrivilege privilege : privileges) {
      if (privilege.getServerName() == null ||
        privilege.getServerName().trim().isEmpty()) {
        throw new SentryInvalidInputException("Invalid Privilege input: Server Name is missing");
      }
      if (privilege.getAction() == null ||
        privilege.getAction().trim().isEmpty()) {
        throw new SentryInvalidInputException("Invalid Privilege input: Action is missing");
      }
    }
  }

  /**
   * Validates grant option in all the privileges.
   *
   * @param privileges Set of privileges to be validated
   * @throws SentryInvalidInputException If the validation for grant option fails for any
   *                                     of the privileges.
   */
  private static void validateGrantOptionInprivileges(Set<TSentryPrivilege> privileges)
    throws SentryInvalidInputException {
    for (TSentryPrivilege privilege : privileges) {
      if (privilege.getGrantOption() == TSentryGrantOption.UNSET) {
        throw new SentryInvalidInputException("Invalid Privilege input," +
          " UNSET option for GRANT <PRIVILEGE> is not valid");
      }
    }
  }
}
