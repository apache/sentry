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
package org.apache.sentry.sqoop;

public class SentrySqoopError {
  public static final String SHOW_GRANT_NOT_SUPPORTED_FOR_PRINCIPAL =
      "Sentry does only support show roles on group, not supported on ";
  public static final String AUTHORIZE_CHECK_NOT_SUPPORT_FOR_PRINCIPAL =
      "Sentry does only support authorization check on user principal, not supported on ";
  public static final String SHOW_PRIVILEGE_NOT_SUPPORTED_FOR_PRINCIPAL =
      "Sentry does only support show privilege on role, not supported on ";
  public static final String GRANT_REVOKE_PRIVILEGE_NOT_SUPPORT_FOR_PRINCIPAL =
      "Sentry does only support grant/revoke privilege to/from role, not supported on ";
  public static final String GRANT_REVOKE_ROLE_NOT_SUPPORT_FOR_PRINCIPAL =
      "Sentry does only support grant/revoke role to/from group, not supported on ";
  public static final String NOT_IMPLEMENT_YET =
      "Sentry does not implement yet ";
}
