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

import static org.apache.sentry.provider.common.ProviderConstants.AUTHORIZABLE_SPLITTER;
import static org.apache.sentry.provider.common.ProviderConstants.PRIVILEGE_PREFIX;

import java.util.List;

import org.apache.sentry.core.model.search.SearchModelAction;
import org.apache.sentry.core.model.search.SearchModelAuthorizable;
import org.apache.sentry.policy.common.PrivilegeValidator;
import org.apache.shiro.config.ConfigurationException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

public abstract class AbstractSearchPrivilegeValidator implements PrivilegeValidator {

  @VisibleForTesting
  public static Iterable<SearchModelAuthorizable> parsePrivilege(String string) {
     Iterable<SearchAuthorizableOrAction> both = parsePrivilegeAndAction(string);
     List<SearchModelAuthorizable> authorizable = Lists.newArrayList();
     for (SearchAuthorizableOrAction or : both) {
       if (or.getAuthorizable() != null) {
         authorizable.add(or.getAuthorizable());
       }
     }
     return authorizable;
  }

  public static Iterable<SearchAuthorizableOrAction> parsePrivilegeAndAction(String string) {
    List<SearchAuthorizableOrAction> result = Lists.newArrayList();
    for(String section : AUTHORIZABLE_SPLITTER.split(string)) {
      // XXX this ugly hack is because action is not an authorizable
      if(!section.toLowerCase().startsWith(PRIVILEGE_PREFIX)) {
        SearchModelAuthorizable authorizable = SearchModelAuthorizables.from(section);
        if(authorizable == null) {
          String msg = "No authorizable found for " + section;
          throw new ConfigurationException(msg);
        }
        result.add(new SearchAuthorizableOrAction(authorizable));
      } else {
        // it's an action
        SearchModelAction action = SearchModelActions.from(section);
        if (action != null) {
          result.add(new SearchAuthorizableOrAction(action));
        }
      }
    }
    return result;
  }

  public static class SearchAuthorizableOrAction {
    private SearchModelAuthorizable authorizable;
    private SearchModelAction action;

    public SearchAuthorizableOrAction(
        SearchModelAuthorizable authorizable) {
      this(authorizable, null);
    }

    public SearchAuthorizableOrAction(
        SearchModelAction action) {
      this(null, action);
    }

    private SearchAuthorizableOrAction(
        SearchModelAuthorizable authorizable,
        SearchModelAction action) {
      this.authorizable = authorizable;
      this.action = action;
    }

    public SearchModelAuthorizable getAuthorizable() { return authorizable; }
    public SearchModelAction getAction() { return action; }
  }
}
