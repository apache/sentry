/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.provider.db.generic.tools;

import static org.apache.sentry.core.common.utils.SentryConstants.AUTHORIZABLE_SEPARATOR;
import static org.apache.sentry.core.common.utils.SentryConstants.KV_SEPARATOR;
import static org.apache.sentry.core.common.utils.SentryConstants.RESOURCE_WILDCARD_VALUE;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.sentry.api.generic.thrift.TAuthorizable;
import org.apache.sentry.api.generic.thrift.TSentryGrantOption;
import org.apache.sentry.api.generic.thrift.TSentryPrivilege;
import org.apache.sentry.core.common.Authorizable;
import org.apache.sentry.core.common.exception.SentryUserException;
import org.apache.sentry.core.common.utils.KeyValue;
import org.apache.sentry.core.common.utils.PolicyFileConstants;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.common.validator.PrivilegeValidator;
import org.apache.sentry.core.common.validator.PrivilegeValidatorContext;
import org.apache.sentry.core.model.indexer.IndexerModelAuthorizables;
import org.apache.sentry.core.model.indexer.IndexerPrivilegeModel;
import org.apache.sentry.core.model.kafka.KafkaAuthorizable;
import org.apache.sentry.core.model.kafka.KafkaModelAuthorizables;
import org.apache.sentry.core.model.kafka.KafkaPrivilegeModel;
import org.apache.sentry.core.model.solr.SolrModelAuthorizables;
import org.apache.sentry.core.model.solr.SolrPrivilegeModel;
import org.apache.sentry.core.model.sqoop.SqoopModelAuthorizables;
import org.apache.sentry.core.model.sqoop.SqoopPrivilegeModel;
import org.apache.sentry.provider.common.AuthorizationComponent;
import org.apache.shiro.config.ConfigurationException;

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

  public GenericPrivilegeConverter(String component, String service) {
    this(component, service, true);
  }

  public GenericPrivilegeConverter(String component, String service, boolean validate) {
    this.component = component;
    this.service = service;
    this.validate = validate;
  }

  public TSentryPrivilege fromString(String privilegeStr) throws SentryUserException {
    privilegeStr = parsePrivilegeString(privilegeStr);
    if (validate) {
      validatePrivilegeHierarchy(privilegeStr);
    }

    TSentryPrivilege tSentryPrivilege = new TSentryPrivilege();
    List<TAuthorizable> authorizables = new LinkedList<TAuthorizable>();
    for (String authorizable : SentryConstants.AUTHORIZABLE_SPLITTER.split(privilegeStr)) {
      KeyValue keyValue = new KeyValue(authorizable);
      String key = keyValue.getKey();
      String value = keyValue.getValue();

      Authorizable authz = getAuthorizable(keyValue);
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

  private String parsePrivilegeString(String privilegeStr) {
    if (AuthorizationComponent.KAFKA.equals(component)) {
      final String hostPrefix = KafkaAuthorizable.AuthorizableType.HOST.name() + KV_SEPARATOR;
      final String hostPrefixLowerCase = hostPrefix.toLowerCase();
      if (!privilegeStr.toLowerCase().startsWith(hostPrefixLowerCase)) {
        return hostPrefix + RESOURCE_WILDCARD_VALUE + AUTHORIZABLE_SEPARATOR + privilegeStr;
      }
    }

    return privilegeStr;
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

  protected List<PrivilegeValidator> getPrivilegeValidators() throws SentryUserException {
    if (AuthorizationComponent.KAFKA.equals(component)) {
      return KafkaPrivilegeModel.getInstance().getPrivilegeValidators();
    } else if ("SOLR".equals(component)) {
      return SolrPrivilegeModel.getInstance().getPrivilegeValidators();
    } else if (AuthorizationComponent.SQOOP.equals(component)) {
      return SqoopPrivilegeModel.getInstance().getPrivilegeValidators(service);
    } else if (AuthorizationComponent.HBASE_INDEXER.equals(component)) {
      return IndexerPrivilegeModel.getInstance().getPrivilegeValidators();
    }

    throw new SentryUserException("Invalid component specified for GenericPrivilegeCoverter: " + component);
  }

  protected Authorizable getAuthorizable(KeyValue keyValue) throws SentryUserException {
    if (AuthorizationComponent.KAFKA.equals(component)) {
      return KafkaModelAuthorizables.from(keyValue);
    } else if ("SOLR".equals(component)) {
      return SolrModelAuthorizables.from(keyValue);
    } else if (AuthorizationComponent.SQOOP.equals(component)) {
      return SqoopModelAuthorizables.from(keyValue);
    } else if (AuthorizationComponent.HBASE_INDEXER.equals(component)) {
      return IndexerModelAuthorizables.from(keyValue);
    }

    throw new SentryUserException("Invalid component specified for GenericPrivilegeCoverter: " + component);
  }

}
