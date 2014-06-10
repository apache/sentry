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
package org.apache.sentry.core.model.db;

import com.google.common.collect.ImmutableSet;

public class AccessConstants {

  /**
   * Used as the &quot;name&quot; for a Server, Database, Table object which
   * represents all Servers, Databases, or Tables.
   */
  public static final String ALL = "*";
  public static final String SOME = "+";

  public static final String SELECT = "select";
  public static final String INSERT = "insert";

  public static final String ALL_ROLE = "ALL", DEFAULT_ROLE = "DEFAULT", NONE_ROLE = "NONE",
      SUPERUSER_ROLE = "SUPERUSER", PUBLIC_ROLE = "PUBLIC";
  public static final ImmutableSet<String> RESERVED_ROLE_NAMES = ImmutableSet.of(ALL_ROLE,
      DEFAULT_ROLE, NONE_ROLE, SUPERUSER_ROLE, PUBLIC_ROLE);
}
