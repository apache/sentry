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

import org.apache.sentry.provider.db.log.util.Constants;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ContainerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuditMetadataLogEntity implements JsonLogEntity {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(AuditMetadataLogEntity.class);
  private static final JsonFactory factory = new MappingJsonFactory();
  private String serviceName;
  private String userName;
  private String impersonator;
  private String ipAddress;
  private String operation;
  private String eventTime;
  private String operationText;
  private String allowed;
  private String databaseName;
  private String tableName;
  private String resourcePath;
  private String objectType;

  public AuditMetadataLogEntity() {
  }

  public AuditMetadataLogEntity(String serviceName, String userName,
      String impersonator, String ipAddress, String operation,
      String eventTime, String operationText, String allowed,
      String databaseName, String tableName, String resourcePath,
      String objectType) {
    this.serviceName = serviceName;
    this.userName = userName;
    this.impersonator = impersonator;
    this.ipAddress = ipAddress;
    this.operation = operation;
    this.eventTime = eventTime;
    this.operationText = operationText;
    this.allowed = allowed;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.resourcePath = resourcePath;
    this.objectType = objectType;
  }

  @Override
  public String toJsonFormatLog() {
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
      json.writeStringField(Constants.LOG_FIELD_OPERATION_TEXT, operationText);
      json.writeStringField(Constants.LOG_FIELD_ALLOWED, allowed);
      json.writeStringField(Constants.LOG_FIELD_DATABASE_NAME, databaseName);
      json.writeStringField(Constants.LOG_FIELD_TABLE_NAME, tableName);
      json.writeStringField(Constants.LOG_FIELD_RESOURCE_PATH, resourcePath);
      json.writeStringField(Constants.LOG_FIELD_OBJECT_TYPE, objectType);
      json.writeEndObject();
      json.flush();
    } catch (IOException e) {
      // if there has error when creating the audit log in json, set the audit
      // log to empty.
      stringWriter = new StringWriter();
      String msg = "Error creating audit log in json format: " + e.getMessage();
      LOGGER.error(msg, e);
    } finally {
      try {
        if (json != null) {
          json.close();
        }
      } catch (IOException e) {
        LOGGER.error("Error closing JsonGenerator", e);
      }
    }

    return stringWriter.toString();
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getImpersonator() {
    return impersonator;
  }

  public void setImpersonator(String impersonator) {
    this.impersonator = impersonator;
  }

  public String getIpAddress() {
    return ipAddress;
  }

  public void setIpAddress(String ipAddress) {
    this.ipAddress = ipAddress;
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public String getEventTime() {
    return eventTime;
  }

  public void setEventTime(String eventTime) {
    this.eventTime = eventTime;
  }

  public String getOperationText() {
    return operationText;
  }

  public void setOperationText(String operationText) {
    this.operationText = operationText;
  }

  public String getAllowed() {
    return allowed;
  }

  public void setAllowed(String allowed) {
    this.allowed = allowed;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getResourcePath() {
    return resourcePath;
  }

  public void setResourcePath(String resourcePath) {
    this.resourcePath = resourcePath;
  }

  public String getObjectType() {
    return objectType;
  }

  public void setObjectType(String objectType) {
    this.objectType = objectType;
  }

  /**
   * For use in tests
   * 
   * @param json
   *          incoming JSON to parse
   * @return a node tree
   * @throws IOException
   *           on any parsing problems
   */
  public static ContainerNode parse(String json) throws IOException {
    ObjectMapper mapper = new ObjectMapper(factory);
    JsonNode jsonNode = mapper.readTree(json);
    if (!(jsonNode instanceof ContainerNode)) {
      throw new IOException("Wrong JSON data: " + json);
    }
    return (ContainerNode) jsonNode;
  }
}
