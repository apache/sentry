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

package org.apache.sentry.provider.file;

import java.util.List;

import org.apache.sentry.core.common.Authorizable;

import com.google.common.collect.ImmutableSetMultimap;

public interface PolicyEngine {

  /**
   * Get permissions associated with a group. Returns Strings which can be resolved
   * by the caller. Strings are returned to separate the PolicyFile class from the
   * type of permissions used in a policy file. Additionally it's possible further
   * processing of the permissions is needed before resolving to a permission object.
   * @param authorizeable object
   * @param group name
   * @return non-null immutable set of permissions
   */
  public ImmutableSetMultimap<String, String> getPermissions(List<? extends Authorizable> authorizables, List<String> groups);

}
