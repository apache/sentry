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

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.MappingJsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ContainerNode;

abstract public class AuditMetadataLogEntity implements JsonLogEntity {

  static final JsonFactory factory = new MappingJsonFactory();
  private String serviceName;
  private String userName;
  private String impersonator;
  private String ipAddress;
  private String operation;
  private String eventTime;
  private String operationText;
  private String allowed;
  private String objectType;
  private String component;

  void setCommonAttr(String serviceName, String userName, String impersonator, String ipAddress,
      String operation, String eventTime, String operationText, String allowed, String objectType,
      String component) {
    this.serviceName = serviceName;
    this.userName = userName;
    this.impersonator = impersonator;
    this.ipAddress = ipAddress;
    this.operation = operation;
    this.eventTime = eventTime;
    this.operationText = operationText;
    this.allowed = allowed;
    this.objectType = objectType;
    this.component = component;
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

  public String getObjectType() {
    return objectType;
  }

  public void setObjectType(String objectType) {
    this.objectType = objectType;
  }

  public String getComponent() {
    return component;
  }

  public void setComponent(String component) {
    this.component = component;
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
