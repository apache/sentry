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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.apache.sentry.provider.db.log.util.Constants;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ContainerNode;
import org.junit.Test;

public class TestGMAuditMetadataLogEntity {
  @Test
  public void testToJsonFormatLog() throws Throwable {

    Map<String, String> privilegesMap = new HashMap<String, String>();
    privilegesMap.put("resourceType1", "resourceName1");
    privilegesMap.put("resourceType2", "resourceName2");
    privilegesMap.put("resourceType3", "resourceName3");
    privilegesMap.put("resourceType4", "resourceName4");
    GMAuditMetadataLogEntity gmamle = new GMAuditMetadataLogEntity("serviceName", "userName",
        "impersonator", "ipAddress", "operation", "eventTime", "operationText", "allowed",
        "objectType", "component", privilegesMap);
    String jsonAuditLog = gmamle.toJsonFormatLog();
    ContainerNode rootNode = AuditMetadataLogEntity.parse(jsonAuditLog);
    assertEntryEquals(rootNode, Constants.LOG_FIELD_SERVICE_NAME, "serviceName");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_USER_NAME, "userName");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_IMPERSONATOR, "impersonator");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_IP_ADDRESS, "ipAddress");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_OPERATION, "operation");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_EVENT_TIME, "eventTime");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_OPERATION_TEXT, "operationText ON COMPONENT component");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_ALLOWED, "allowed");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_OBJECT_TYPE, "objectType");
    // "component" and arbitrary resource names are disabled because of
    // Navigator log format
    // assertEntryEquals(rootNode, Constants.LOG_FIELD_COMPONENT, "component");
    // assertEntryEquals(rootNode, "resourceType1", "resourceName1");
    // assertEntryEquals(rootNode, "resourceType2", "resourceName2");
    // assertEntryEquals(rootNode, "resourceType3", "resourceName3");
    // assertEntryEquals(rootNode, "resourceType4", "resourceName4");
  }

  void assertEntryEquals(ContainerNode rootNode, String key, String value) {
    JsonNode node = assertNodeContains(rootNode, key);
    assertEquals(value, node.getTextValue());
  }

  private JsonNode assertNodeContains(ContainerNode rootNode, String key) {
    JsonNode node = rootNode.get(key);
    if (node == null) {
      fail("No entry of name \"" + key + "\" found in " + rootNode.toString());
    }
    return node;
  }
}
