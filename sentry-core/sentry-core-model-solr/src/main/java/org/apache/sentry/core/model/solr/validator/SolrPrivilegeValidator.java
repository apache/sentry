/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.sentry.core.model.solr.validator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.common.validator.PrivilegeValidator;
import org.apache.sentry.core.common.validator.PrivilegeValidatorContext;
import org.apache.sentry.core.model.solr.AdminOperation;
import org.apache.sentry.core.model.solr.SolrConstants;
import org.apache.shiro.config.ConfigurationException;

/**
 * This class provides the privilege validation functionality for
 * Sentry/Solr permissions.
 */
public class SolrPrivilegeValidator implements PrivilegeValidator {
  private static final Pattern PRIVILEGE_AUTHORIZABLE_REGEX =
      Pattern.compile("^(collection|admin|schema|config)\\s*=\\s*(\\S+)$", Pattern.CASE_INSENSITIVE);;
  private static final Pattern PRIVILEGE_ACTION_REGEX =
      Pattern.compile("^action\\s*=\\s*(query|update|\\*)$", Pattern.CASE_INSENSITIVE);

  private String entityType;
  private String entityName;
  private String actionName;

  @Override
  public void validate(PrivilegeValidatorContext context) throws ConfigurationException {
    try {
      validate(context.getPrivilege(), false);
    } catch (IllegalArgumentException ex) {
      throw new ConfigurationException(ex.getMessage());
    }
  }

  public void validate (String privilegeStr, boolean actionRequired) {
    String[] components = privilegeStr.split(SentryConstants.AUTHORIZABLE_SEPARATOR);
    Matcher authMatcher = PRIVILEGE_AUTHORIZABLE_REGEX.matcher(components[0].trim());

    if (!authMatcher.matches()) {
      throw new IllegalArgumentException("Invalid privilege String: " + privilegeStr);
    }

    entityType = authMatcher.group(1).toLowerCase();
    entityName = authMatcher.group(2).toLowerCase();
    actionName = null;

    if (components.length > 1) {
      Matcher actionMactcher = PRIVILEGE_ACTION_REGEX.matcher(components[1].trim());
      if (actionMactcher.matches()) {
        actionName = actionMactcher.group(1).toLowerCase();
      } else {
        throw new IllegalArgumentException("Invalid privilege String: " + privilegeStr);
      }
    }

    if (actionRequired && actionName == null) {
      throw new IllegalArgumentException("Privilege is invalid: action required but not specified.");
    }

    extraPrivilegeValidation (entityType, entityName, actionName);
  }

  private void extraPrivilegeValidation(String entityType, String entityName, String actionName) {
    if ("admin".equals(entityType)) {
      if (!AdminOperation.ENTITY_NAMES.contains(entityName)) {
        throw new IllegalArgumentException(
            "Invalid entity name specified for the admin entity type. Valid names are " + AdminOperation.ENTITY_NAMES);
      } else if (AdminOperation.METRICS.getName().equals(entityName) && !SolrConstants.QUERY.equals(actionName)) {
        throw new IllegalArgumentException(
            "Invalid action specified for the metrics entity of type admin. Valid actions are [" + SolrConstants.QUERY + "]" );
      }
    }
  }

  public String getEntityType() {
    return entityType;
  }

  public String getEntityName() {
    return entityName;
  }

  public String getActionName() {
    return actionName;
  }
}
