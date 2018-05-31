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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.sentry.provider.db.log.util.Constants;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ContainerNode;
import org.junit.Test;

public class TestDbAuditMetadataLogEntity {

  @Test
  public void testToJsonFormatLog() throws Throwable {
    DBAuditMetadataLogEntity amle = new DBAuditMetadataLogEntity("serviceName", "userName",
        "impersonator", "ipAddress", "operation", "eventTime", "operationText", "allowed",
        "objectType", "component", "databaseName", "tableName", "columnName", "resourcePath");
    String jsonAuditLog = amle.toJsonFormatLog();
    ContainerNode rootNode = AuditMetadataLogEntity.parse(jsonAuditLog);
    assertEntryEquals(rootNode, Constants.LOG_FIELD_SERVICE_NAME, "serviceName");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_USER_NAME, "userName");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_IMPERSONATOR,
        "impersonator");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_IP_ADDRESS, "ipAddress");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_OPERATION, "operation");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_EVENT_TIME, "eventTime");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_OPERATION_TEXT,
        "operationText");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_ALLOWED, "allowed");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_DATABASE_NAME,
        "databaseName");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_TABLE_NAME, "tableName");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_COLUMN_NAME, "columnName");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_RESOURCE_PATH,
        "resourcePath");
    assertEntryEquals(rootNode, Constants.LOG_FIELD_OBJECT_TYPE, "objectType");
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
