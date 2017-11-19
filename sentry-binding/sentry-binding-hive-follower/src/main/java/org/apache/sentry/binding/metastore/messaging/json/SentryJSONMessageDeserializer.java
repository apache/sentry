/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.binding.metastore.messaging.json;

import org.apache.hive.hcatalog.messaging.*;
import org.apache.hive.hcatalog.messaging.json.JSONAlterIndexMessage;
import org.apache.hive.hcatalog.messaging.json.JSONCreateFunctionMessage;
import org.apache.hive.hcatalog.messaging.json.JSONCreateIndexMessage;
import org.apache.hive.hcatalog.messaging.json.JSONDropFunctionMessage;
import org.apache.hive.hcatalog.messaging.json.JSONDropIndexMessage;
import org.apache.hive.hcatalog.messaging.json.JSONInsertMessage;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;

public class SentryJSONMessageDeserializer extends MessageDeserializer {
  private static ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public SentryJSONMessageDeserializer() {
  }

  /**
   * Method to de-serialize CreateDatabaseMessage instance.
   */
  @Override
  public SentryJSONCreateDatabaseMessage getCreateDatabaseMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONCreateDatabaseMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONCreateDatabaseMessage: ", e);
    }
  }

  /**
   * Method to de-serialize DropDatabaseMessage instance.
   */
  @Override
  public SentryJSONDropDatabaseMessage getDropDatabaseMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONDropDatabaseMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONDropDatabaseMessage: ", e);
    }
  }

  /**
   * Method to de-serialize CreateTableMessage instance.
   */
  @Override
  public SentryJSONCreateTableMessage getCreateTableMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONCreateTableMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONCreateTableMessage: ", e);
    }
  }

  /**
   * Method to de-serialize AlterTableMessage instance.
   */
  @Override
  public SentryJSONAlterTableMessage getAlterTableMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONAlterTableMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONAlterTableMessage: ", e);
    }
  }

  /**
   * Method to de-serialize DropTableMessage instance.
   */
  @Override
  public SentryJSONDropTableMessage getDropTableMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONDropTableMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONDropTableMessage: ", e);
    }
  }

  /**
   * Method to de-serialize AddPartitionMessage instance.
   */
  @Override
  public SentryJSONAddPartitionMessage getAddPartitionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONAddPartitionMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONAddPartitionMessage: ", e);
    }
  }

  /**
   * Method to de-serialize AlterPartitionMessage instance.
   */
  @Override
  public SentryJSONAlterPartitionMessage getAlterPartitionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONAlterPartitionMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONAlterPartitionMessage: ", e);
    }
  }

  /**
   * Method to de-serialize DropPartitionMessage instance.
   */
  @Override
  public SentryJSONDropPartitionMessage getDropPartitionMessage(String messageBody) {
    try {
      return mapper.readValue(messageBody, SentryJSONDropPartitionMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct SentryJSONDropPartitionMessage: ", e);
    }
  }

  /**
   * Method to de-serialize CreateFunctionMessage instance.
   */
  @Override
  public CreateFunctionMessage getCreateFunctionMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONCreateFunctionMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct JSONCreateFunctionMessage: ", e);
    }
  }

  /**
   * Method to de-serialize DropFunctionMessage instance.
   */
  @Override
  public DropFunctionMessage getDropFunctionMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONDropFunctionMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct JSONDropDatabaseMessage: ", e);
    }
  }

  /**
   * Method to de-serialize CreateIndexMessage instance.                                                                                                                                                   +   */
  @Override
  public CreateIndexMessage getCreateIndexMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONCreateIndexMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct JSONCreateIndexMessage: ", e);
    }
  }

  /**
   * Method to de-serialize DropIndexMessage instance.
   */
  @Override
  public DropIndexMessage getDropIndexMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONDropIndexMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct JSONDropIndexMessage: ", e);
    }
  }

  /**
   * Method to de-serialize AlterIndexMessage instance.
   */
  @Override
  public AlterIndexMessage getAlterIndexMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONAlterIndexMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct JSONAlterIndexMessage: ", e);
    }
  }

  /**
   * Method to de-serialize JSONInsertMessage instance.
   */
  @Override
  public InsertMessage getInsertMessage(String messageBody) {
    // Sentry does not need this message, but it needs to be implemented so that Hive can
    // complete the notification log for such event.
    try {
      return mapper.readValue(messageBody, JSONInsertMessage.class);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not construct InsertMessage", e);
    }
  }

  public static String serialize(Object object) {
    try {
      return mapper.writeValueAsString(object);
    } catch (Exception exception) {
      throw new IllegalArgumentException("Could not serialize: ", exception);
    }
  }
}
