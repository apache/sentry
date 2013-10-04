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

import javax.annotation.Nullable;

import org.apache.sentry.core.model.db.DBModelAuthorizable;
import org.apache.sentry.core.model.db.Server;
import org.apache.shiro.config.ConfigurationException;

public class ServersAllIsInvalid extends AbstractDBRoleValidator {

  @Override
  public void validate(@Nullable String database, String role) throws ConfigurationException {
    Iterable<DBModelAuthorizable> authorizables = parseRole(role);
    for(DBModelAuthorizable authorizable : authorizables) {
      if(authorizable instanceof Server &&
          authorizable.getName().equals(Server.ALL.getName())) {
        String msg = "Invalid value for " + authorizable.getAuthzType() + " in " + role;
        throw new ConfigurationException(msg);
      }
    }
  }

}
