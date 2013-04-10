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

import static org.apache.access.provider.file.PolicyFileConstants.*;

import java.util.List;

import org.apache.access.core.Authorizable;
import org.apache.shiro.config.ConfigurationException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

public abstract class AbstractRoleValidator implements RoleValidator {

  private static final String ACTION_PREFIX = (PRIVILEGE_NAME + KV_SEPARATOR).toLowerCase();

  @VisibleForTesting
  public static Iterable<Authorizable> parseRole(String string) {
    List<Authorizable> result = Lists.newArrayList();
    for(String section : AUTHORIZABLE_SPLITTER.split(string)) {
      // action is not an authorizeable and should be ignored
      if(!section.toLowerCase().startsWith(ACTION_PREFIX)) {
        Authorizable authorizable = Authorizables.from(section);
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
