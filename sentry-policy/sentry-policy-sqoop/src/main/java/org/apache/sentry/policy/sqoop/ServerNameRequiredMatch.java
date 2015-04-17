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
package org.apache.sentry.policy.sqoop;

import static org.apache.sentry.provider.common.ProviderConstants.AUTHORIZABLE_SPLITTER;
import static org.apache.sentry.provider.common.ProviderConstants.PRIVILEGE_PREFIX;

import java.util.List;

import org.apache.sentry.core.model.sqoop.Server;
import org.apache.sentry.core.model.sqoop.SqoopAuthorizable;
import org.apache.sentry.policy.common.PrivilegeValidatorContext;
import org.apache.sentry.policy.common.PrivilegeValidator;
import org.apache.shiro.config.ConfigurationException;

import com.google.common.collect.Lists;

public class ServerNameRequiredMatch implements PrivilegeValidator {
  private final String sqoopServerName;
  public ServerNameRequiredMatch(String sqoopServerName) {
    this.sqoopServerName = sqoopServerName;
  }
  @Override
  public void validate(PrivilegeValidatorContext context)
      throws ConfigurationException {
    Iterable<SqoopAuthorizable> authorizables = parsePrivilege(context.getPrivilege());
    boolean match = false;
    for (SqoopAuthorizable authorizable : authorizables) {
      if ((authorizable instanceof Server) && authorizable.getName().equalsIgnoreCase(sqoopServerName)) {
        match = true;
        break;
      }
    }
    if (!match) {
      String msg = "server=[name] in " + context.getPrivilege()
          + " is required. The name is expected " + sqoopServerName;
      throw new ConfigurationException(msg);
    }
  }

  private Iterable<SqoopAuthorizable> parsePrivilege(String string) {
    List<SqoopAuthorizable> result = Lists.newArrayList();
    for(String section : AUTHORIZABLE_SPLITTER.split(string)) {
      if(!section.toLowerCase().startsWith(PRIVILEGE_PREFIX)) {
        SqoopAuthorizable authorizable = SqoopModelAuthorizables.from(section);
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
