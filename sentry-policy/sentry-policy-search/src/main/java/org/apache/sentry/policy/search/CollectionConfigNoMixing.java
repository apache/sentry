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
package org.apache.sentry.policy.search;

import org.apache.sentry.core.common.SentryConfigurationException;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.core.model.search.Config;
import org.apache.sentry.core.model.search.SearchModelAuthorizable;
import org.apache.sentry.policy.common.PrivilegeValidatorContext;

public class CollectionConfigNoMixing extends AbstractSearchPrivilegeValidator {

  @Override
  public void validate(PrivilegeValidatorContext context) throws SentryConfigurationException {
    String privilege = context.getPrivilege();
    Iterable<SearchModelAuthorizable> authorizables = parsePrivilege(privilege);
    boolean foundCollection = false;
    boolean foundConfig = false;

    for(SearchModelAuthorizable authorizable : authorizables) {
      if(authorizable instanceof Collection) {
        foundCollection = true;
      } else if (authorizable instanceof Config) {
        foundConfig = true;
      }
    }
    if(foundCollection && foundConfig) {
      String msg = "Collection and Config not allowed in same privilege: " + privilege;
      throw new SentryConfigurationException(msg);
    }
  }
}
