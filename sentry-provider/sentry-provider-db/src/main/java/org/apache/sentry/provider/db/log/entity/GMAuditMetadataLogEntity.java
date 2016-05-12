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

package org.apache.sentry.provider.db.log.entity;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.sentry.provider.db.log.util.Constants;
import org.codehaus.jackson.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GMAuditMetadataLogEntity extends AuditMetadataLogEntity {

  private static final Logger LOGGER = LoggerFactory.getLogger(GMAuditMetadataLogEntity.class);
  private static final Set<String> opsWithoutComponent =
      new HashSet<String>(Arrays.asList(Constants.OPERATION_CREATE_ROLE,
                                        Constants.OPERATION_DROP_ROLE,
                                        Constants.OPERATION_ADD_ROLE,
                                        Constants.OPERATION_DELETE_ROLE));

  private Map<String, String> privilegesMap;

  public GMAuditMetadataLogEntity() {
    privilegesMap = new LinkedHashMap<String, String>();
  }

  public GMAuditMetadataLogEntity(String serviceName, String userName, String impersonator,
      String ipAddress, String operation, String eventTime, String operationText, String allowed,
      String objectType, String component, Map<String, String> privilegesMap) {
    setCommonAttr(serviceName, userName, impersonator, ipAddress, operation, eventTime,
        operationText, allowed, objectType, component);
    this.privilegesMap = privilegesMap;
  }

  @Override
  public String toJsonFormatLog() throws Exception {
    StringWriter stringWriter = new StringWriter();
    JsonGenerator json = null;
    try {
      json = factory.createJsonGenerator(stringWriter);
      json.writeStartObject();
      json.writeStringField(Constants.LOG_FIELD_SERVICE_NAME, serviceName);
      json.writeStringField(Constants.LOG_FIELD_USER_NAME, userName);
      json.writeStringField(Constants.LOG_FIELD_IMPERSONATOR, impersonator);
      json.writeStringField(Constants.LOG_FIELD_IP_ADDRESS, ipAddress);
      json.writeStringField(Constants.LOG_FIELD_OPERATION, operation);
      json.writeStringField(Constants.LOG_FIELD_EVENT_TIME, eventTime);

      // FixMe: since we can't use component (see below), add it to
      // operationText here, if relevant

      StringBuilder operationTextSB = new StringBuilder(operationText);
      if (!opsWithoutComponent.contains(operation)) {
        operationTextSB.append(" ON COMPONENT " + component);
      }
      json.writeStringField(Constants.LOG_FIELD_OPERATION_TEXT,
          operationTextSB.toString());
      json.writeStringField(Constants.LOG_FIELD_ALLOWED, allowed);

      // FixMe: we need to conform to the Navigator format, which has the
      // following log fields below.  Use them if they exist in the privilege,
      // otherwise just output null.
      Map<String, Boolean> privHasObject = new LinkedHashMap<String, Boolean>();
      privHasObject.put(Constants.LOG_FIELD_DATABASE_NAME, false);
      privHasObject.put(Constants.LOG_FIELD_TABLE_NAME, false);
      privHasObject.put(Constants.LOG_FIELD_COLUMN_NAME, false);
      privHasObject.put(Constants.LOG_FIELD_RESOURCE_PATH, false);

      for (Map.Entry<String, String> entry : privilegesMap.entrySet()) {
        if (privHasObject.containsKey(entry.getKey())) {
          json.writeStringField(entry.getKey(), entry.getValue());
          privHasObject.put(entry.getKey(), true);
        }
      }

      for (Map.Entry<String, Boolean> entry : privHasObject.entrySet()) {
        if (Boolean.FALSE.equals(entry.getValue())) {
          json.writeStringField(entry.getKey(), null);
        }
      }
      json.writeStringField(Constants.LOG_FIELD_OBJECT_TYPE, objectType);
      // FixMe: component not in navigator schema
      //json.writeStringField(Constants.LOG_FIELD_COMPONENT, component);
      json.writeEndObject();
      json.flush();
    } catch (IOException e) {
      String msg = "Error creating audit log in json format: " + e.getMessage();
      LOGGER.error(msg, e);
      throw e;
    } finally {
      try {
        if (json != null) {
          json.close();
        }
      } catch (IOException e) {
        throw e;
      }
    }

    return stringWriter.toString();
  }

  public Map<String, String> getPrivilegesMap() {
    return privilegesMap;
  }

  public void setPrivilegesMap(Map<String, String> privilegesMap) {
    this.privilegesMap = privilegesMap;
  }

}
