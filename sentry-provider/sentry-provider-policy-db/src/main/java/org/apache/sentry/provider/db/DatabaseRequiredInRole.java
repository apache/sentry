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

import org.apache.sentry.core.AccessURI;
import org.apache.sentry.core.Authorizable;
import org.apache.sentry.core.Database;
import org.apache.shiro.config.ConfigurationException;

public class DatabaseRequiredInRole extends AbstractDBRoleValidator {

  @Override
  public void validate(@Nullable String database, String role) throws ConfigurationException {
    /*
     *  Rule only applies to rules in per database policy file
     */
    if(database != null) {
      Iterable<Authorizable> authorizables = parseRole(role);
      /*
       * Each permission in a non-global file must have a database
       * object except for URIs.
       *
       * We allow URIs to be specified in the per DB policy file for
       * ease of mangeability. URIs will contain to remain server scope
       * objects.
       */
      boolean foundDatabaseInAuthorizables = false;
      boolean foundURIInAuthorizables = false;
      boolean allowURIInAuthorizables = false;

      if ("true".equalsIgnoreCase(
          System.getProperty(SimpleDBPolicyEngine.ACCESS_ALLOW_URI_PER_DB_POLICYFILE))) {
        allowURIInAuthorizables = true;
      }

      for(Authorizable authorizable : authorizables) {
        if(authorizable instanceof Database) {
          foundDatabaseInAuthorizables = true;
        }
        if (authorizable instanceof AccessURI) {
          if (foundDatabaseInAuthorizables) {
            String msg = "URI object is specified at DB scope in " + role;
            throw new ConfigurationException(msg);
          }
          foundURIInAuthorizables = true;
        }
      }
      if(!foundDatabaseInAuthorizables && !(foundURIInAuthorizables && allowURIInAuthorizables)) {
        String msg = "Missing database object in " + role;
        throw new ConfigurationException(msg);
      }
    }
  }
}
