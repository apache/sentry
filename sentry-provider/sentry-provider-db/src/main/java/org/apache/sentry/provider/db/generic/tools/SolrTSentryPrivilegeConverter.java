/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.db.generic.tools;

import com.google.common.collect.Lists;

import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.search.Collection;
import org.apache.sentry.core.model.search.SearchModelAuthorizable;
import org.apache.sentry.core.common.validator.PrivilegeValidator;
import org.apache.sentry.core.common.validator.PrivilegeValidatorContext;
import org.apache.sentry.core.model.search.SearchModelAuthorizables;
import org.apache.sentry.core.model.search.SearchPrivilegeModel;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.PolicyFileConstants;
import org.apache.sentry.provider.db.generic.service.thrift.TAuthorizable;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.generic.tools.command.TSentryPrivilegeConverter;
import org.apache.shiro.config.ConfigurationException;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public  class SolrTSentryPrivilegeConverter implements TSentryPrivilegeConverter {
  private String component;
  private String service;
  private boolean validate;

  public SolrTSentryPrivilegeConverter(String component, String service) {
    this(component, service, true);
  }

  public SolrTSentryPrivilegeConverter(String component, String service, boolean validate) {
    this.component = component;
    this.service = service;
    this.validate = validate;
  }

  public TSentryPrivilege fromString(String privilegeStr) throws Exception {
    if (validate) {
      validatePrivilegeHierarchy(privilegeStr);
    }

    TSentryPrivilege tSentryPrivilege = new TSentryPrivilege();
    List<TAuthorizable> authorizables = new LinkedList<TAuthorizable>();
    for (String authorizable : SentryConstants.AUTHORIZABLE_SPLITTER.split(privilegeStr)) {
      KeyValue keyValue = new KeyValue(authorizable);
      String key = keyValue.getKey();
      String value = keyValue.getValue();

      // is it an authorizable?
      SearchModelAuthorizable authz = SearchModelAuthorizables.from(keyValue);
      if (authz != null) {
        if (authz instanceof Collection) {
          Collection coll = (Collection)authz;
          authorizables.add(new TAuthorizable(coll.getTypeName(), coll.getName()));
        } else {
          throw new IllegalArgumentException("Unknown authorizable type: " + authz.getTypeName());
        }
      } else if (PolicyFileConstants.PRIVILEGE_ACTION_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setAction(value);
      // Limitation: don't support grant at this time, since the existing solr use cases don't need it.
      } else {
        throw new IllegalArgumentException("Unknown key: " + key);
      }
    }

    if (tSentryPrivilege.getAction() == null) {
      throw new IllegalArgumentException("Privilege is invalid: action required but not specified.");
    }
    tSentryPrivilege.setComponent(component);
    tSentryPrivilege.setServiceName(service);
    tSentryPrivilege.setAuthorizables(authorizables);
    return tSentryPrivilege;
  }

  public String toString(TSentryPrivilege tSentryPrivilege) {
    List<String> privileges = Lists.newArrayList();
    if (tSentryPrivilege != null) {
      List<TAuthorizable> authorizables = tSentryPrivilege.getAuthorizables();
      String action = tSentryPrivilege.getAction();
      String grantOption = (tSentryPrivilege.getGrantOption() == TSentryGrantOption.TRUE ? "true"
              : "false");

      Iterator<TAuthorizable> it = authorizables.iterator();
      if (it != null) {
        while (it.hasNext()) {
          TAuthorizable tAuthorizable = it.next();
          privileges.add(SentryConstants.KV_JOINER.join(
              tAuthorizable.getType(), tAuthorizable.getName()));
        }
      }

      if (!authorizables.isEmpty()) {
        privileges.add(SentryConstants.KV_JOINER.join(
            PolicyFileConstants.PRIVILEGE_ACTION_NAME, action));
      }

      // only append the grant option to privilege string if it's true
      if ("true".equals(grantOption)) {
        privileges.add(SentryConstants.KV_JOINER.join(
            PolicyFileConstants.PRIVILEGE_GRANT_OPTION_NAME, grantOption));
      }
    }
    return SentryConstants.AUTHORIZABLE_JOINER.join(privileges);
  }

  private static void validatePrivilegeHierarchy(String privilegeStr) throws Exception {
    List<PrivilegeValidator> validators = SearchPrivilegeModel.getInstance().getPrivilegeValidators();
    PrivilegeValidatorContext context = new PrivilegeValidatorContext(null, privilegeStr);
    for (PrivilegeValidator validator : validators) {
      try {
        validator.validate(context);
      } catch (ConfigurationException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }
}
