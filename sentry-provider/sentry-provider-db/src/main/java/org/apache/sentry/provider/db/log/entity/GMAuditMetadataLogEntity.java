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
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.sentry.provider.db.log.util.Constants;
import org.codehaus.jackson.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GMAuditMetadataLogEntity extends AuditMetadataLogEntity {

  private static final Logger LOGGER = LoggerFactory.getLogger(GMAuditMetadataLogEntity.class);
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
      json.writeStringField(Constants.LOG_FIELD_SERVICE_NAME, getServiceName());
      json.writeStringField(Constants.LOG_FIELD_USER_NAME, getUserName());
      json.writeStringField(Constants.LOG_FIELD_IMPERSONATOR, getImpersonator());
      json.writeStringField(Constants.LOG_FIELD_IP_ADDRESS, getIpAddress());
      json.writeStringField(Constants.LOG_FIELD_OPERATION, getOperation());
      json.writeStringField(Constants.LOG_FIELD_EVENT_TIME, getEventTime());
      json.writeStringField(Constants.LOG_FIELD_OPERATION_TEXT, getOperationText());
      json.writeStringField(Constants.LOG_FIELD_ALLOWED, getAllowed());
      for (Map.Entry<String, String> entry : privilegesMap.entrySet()) {
        json.writeStringField(entry.getKey(), entry.getValue());
      }
      json.writeStringField(Constants.LOG_FIELD_OBJECT_TYPE, getObjectType());
      json.writeStringField(Constants.LOG_FIELD_COMPONENT, getComponent());
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
        String msg = "Error when close json object: " + e.getMessage();
        LOGGER.error(msg, e);
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
