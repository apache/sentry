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
package org.apache.sentry.provider.db;

import static org.apache.sentry.provider.file.PolicyFileConstants.AUTHORIZABLE_SPLITTER;
import static org.apache.sentry.provider.file.PolicyFileConstants.PRIVILEGE_PREFIX;

import java.util.List;

import org.apache.sentry.provider.common.RoleValidator;
import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.shiro.config.ConfigurationException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

public abstract class AbstractDBRoleValidator implements RoleValidator {

  @VisibleForTesting
  public static Iterable<DBModelAuthorizable> parseRole(String string) {
    List<DBModelAuthorizable> result = Lists.newArrayList();
    for(String section : AUTHORIZABLE_SPLITTER.split(string)) {
      // XXX this ugly hack is because action is not an authorizeable
      if(!section.toLowerCase().startsWith(PRIVILEGE_PREFIX)) {
        DBModelAuthorizable authorizable = DBModelAuthorizables.from(section);
        if(authorizable == null) {
          String msg = "No authorizable found for " + section;
          throw new ConfigurationException(msg);
        }
        result.add(authorizable);
      }
    }
    return result;
  }

}
