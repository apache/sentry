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
package org.apache.access.provider.file;

import javax.annotation.Nullable;

import org.apache.access.core.Authorizable;
import org.apache.access.core.Database;
import org.apache.shiro.config.ConfigurationException;

public class DatabaseRequiredInRole extends AbstractRoleValidator {

  @Override
  public void validate(@Nullable String database, String role) throws ConfigurationException {
    /*
     *  Rule only applies to rules in per database policy file
     */
    if(database != null) {
      Iterable<Authorizable> authorizables = parseRole(role);
      /*
       * Each permission in a non-global file must have a database
       * object.
       */
      boolean foundDatabaseInAuthorizables = false;
      for(Authorizable authorizable : authorizables) {
        if(authorizable instanceof Database) {
          foundDatabaseInAuthorizables = true;
          break;
        }
      }
      if(!foundDatabaseInAuthorizables) {
        String msg = "Missing database object in " + role;
        throw new ConfigurationException(msg);
      }
    }
  }
}
