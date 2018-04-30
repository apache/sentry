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
package org.apache.sentry.cli.tools;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.utils.SentryConstants;
import org.apache.sentry.core.model.solr.validator.SolrPrivilegeValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides SOLR specific functionality required for migrating Sentry privileges.
 */
public class PermissionsMigrationToolSolr extends PermissionsMigrationToolCommon {
  private static final Logger LOGGER = LoggerFactory.getLogger(PermissionsMigrationToolSolr.class);


  @Override
  protected String getComponent(Configuration conf) {
    return "SOLR";
  }

  @Override
  protected String getServiceName(Configuration conf) {
    return conf.get(SOLR_SERVICE_NAME, "service1");
  }

  @Override
  protected Collection<String> transformPrivileges(Collection<String> privileges) {
    List<String> result = new ArrayList<>();
    boolean migrated = false;

    if (getSourceVersion().major == 1) { // Migrate only Sentry 1.x permissions
      for (String p : privileges) {
        SolrPrivilegeValidator v = new SolrPrivilegeValidator();
        v.validate(p, false);

        if ("collection".equalsIgnoreCase(v.getEntityType()) && "admin".equalsIgnoreCase(v.getEntityName())) {
          result.add(getPermissionStr("admin", "collections", v.getActionName()));
          result.add(getPermissionStr("admin", "cores", v.getActionName()));
          migrated = true;
        } else if ("collection".equalsIgnoreCase(v.getEntityType()) && "*".equals(v.getEntityName())) {
          result.add(getPermissionStr("admin", "collections", v.getActionName()));
          result.add(getPermissionStr("admin", "cores", v.getActionName()));
          result.add(p);
          migrated = true;
        } else {
          result.add(p);
        }
      }
    }

    return migrated ? result : Collections.emptyList();
  }

  private String getPermissionStr (String entityType, String entityName, String action) {
    StringBuilder builder = new StringBuilder();
    builder.append(entityType);
    builder.append(SentryConstants.KV_SEPARATOR);
    builder.append(entityName);
    if (action != null) {
      builder.append(SentryConstants.AUTHORIZABLE_SEPARATOR);
      builder.append(SentryConstants.PRIVILEGE_NAME);
      builder.append(SentryConstants.KV_SEPARATOR);
      builder.append(action);
    }
    return builder.toString();
  }

  public static void main(String[] args) throws Exception {
    PermissionsMigrationToolSolr solrTool = new PermissionsMigrationToolSolr();
    try {
      solrTool.executeConfigTool(args);
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      Throwable current = e;
      // find the first printable message;
      while (current != null && current.getMessage() == null) {
        current = current.getCause();
      }
      String error = "";
      if (current != null && current.getMessage() != null) {
        error = "Message: " + current.getMessage();
      }
      System.out.println("The operation failed. " + error);
      System.exit(1);
    }
  }
}
