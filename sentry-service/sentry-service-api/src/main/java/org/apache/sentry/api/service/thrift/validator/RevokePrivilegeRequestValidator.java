/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.api.service.thrift.validator;

import org.apache.sentry.core.common.exception.SentryInvalidInputException;
import org.apache.sentry.api.service.thrift.TAlterSentryRoleRevokePrivilegeRequest;

/**
 * Check's for mandatory fields in the privileges
 */
public final class RevokePrivilegeRequestValidator {
  private RevokePrivilegeRequestValidator() {
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
  public static void validate(TAlterSentryRoleRevokePrivilegeRequest request)
    throws SentryInvalidInputException {
    if (request.isSetPrivileges() && (!request.getPrivileges().isEmpty())) {
      GrantPrivilegeRequestValidator.checkForMandatoryFieldsInPrivileges(request.getPrivileges());
    }
  }
}
