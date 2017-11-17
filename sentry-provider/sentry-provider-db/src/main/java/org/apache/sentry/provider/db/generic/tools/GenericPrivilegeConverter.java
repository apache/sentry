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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.AuthorizableFactory;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.PolicyFileConstants;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.common.validator.PrivilegeValidator;
import org.apache.sentry.core.common.validator.PrivilegeValidatorContext;
import org.apache.sentry.provider.db.generic.service.thrift.TAuthorizable;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryGrantOption;
import org.apache.sentry.provider.db.generic.service.thrift.TSentryPrivilege;
import org.apache.sentry.provider.db.generic.tools.command.TSentryPrivilegeConverter;
import org.apache.shiro.config.ConfigurationException;

import java.util.ArrayList;
import java.util.List;

/**
 * A TSentryPrivilegeConverter implementation for "Generic" privileges, covering Apache Kafka, Apache Solr and Apache Sqoop.
 * It converts privilege Strings to TSentryPrivilege Objects, and vice versa, for Generic clients.
 *
 * When a privilege String is converted to a TSentryPrivilege in "fromString", the validators associated with the
 * given privilege model are also called on the privilege String.
 */
public class GenericPrivilegeConverter implements TSentryPrivilegeConverter {
  private String component;
  private String service;
  private boolean validate;

  private List<PrivilegeValidator> privilegeValidators;

  private AuthorizableFactory authorizableFactory;

  /**
   * Optional function to parse or convert privilege string.
   */
  private Function<String, String> privilegeStrParser;

  public GenericPrivilegeConverter(String component, String service, List<PrivilegeValidator> privilegeValidators, AuthorizableFactory authorizableFactory, boolean validate) {
    this.component = component;
    this.service = service;
    this.privilegeValidators = privilegeValidators;
    this.authorizableFactory = authorizableFactory;
    this.validate = validate;
  }


  public TSentryPrivilege fromString(String privilegeStr) throws SentryUserException {
    privilegeStr = parsePrivilegeString(privilegeStr);
    if (validate) {
      validatePrivilegeHierarchy(privilegeStr);
    }

    TSentryPrivilege tSentryPrivilege = new TSentryPrivilege();
    List<TAuthorizable> authorizables = new ArrayList<>();
    for (String authorizable : SentryConstants.AUTHORIZABLE_SPLITTER.split(privilegeStr)) {
      KeyValue keyValue = new KeyValue(authorizable);
      String key = keyValue.getKey();
      String value = keyValue.getValue();

      Authorizable authz = authorizableFactory.create(key, value);
      if (authz != null) {
        authorizables.add(new TAuthorizable(authz.getTypeName(), authz.getName()));
      } else if (PolicyFileConstants.PRIVILEGE_ACTION_NAME.equalsIgnoreCase(key)) {
        tSentryPrivilege.setAction(value);
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

      for (TAuthorizable tAuthorizable : authorizables) {
        privileges.add(SentryConstants.KV_JOINER.join(
                tAuthorizable.getType(), tAuthorizable.getName()));
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

  private String parsePrivilegeString(String privilegeStr) {
    if (privilegeStrParser == null) {
      return privilegeStr;
    }
    return privilegeStrParser.apply(privilegeStr);
  }

  private void validatePrivilegeHierarchy(String privilegeStr) throws SentryUserException {
    List<PrivilegeValidator> validators = getPrivilegeValidators();
    PrivilegeValidatorContext context = new PrivilegeValidatorContext(null, privilegeStr);
    for (PrivilegeValidator validator : validators) {
      try {
        validator.validate(context);
      } catch (ConfigurationException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  private List<PrivilegeValidator> getPrivilegeValidators() {
    return privilegeValidators;
  }


  public void setPrivilegeValidators(List<PrivilegeValidator> privilegeValidators) {
    this.privilegeValidators = privilegeValidators;
  }

  public void setAuthorizableFactory(AuthorizableFactory authorizableFactory) {
    this.authorizableFactory = authorizableFactory;
  }

  public void setPrivilegeStrParser(Function<String, String> privilegeStrParser) {
    this.privilegeStrParser = privilegeStrParser;
  }

}
