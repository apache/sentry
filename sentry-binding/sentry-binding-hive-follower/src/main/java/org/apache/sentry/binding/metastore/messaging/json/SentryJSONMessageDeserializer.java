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

package org.apache.sentry.binding.metastore.messaging.json;

import org.apache.hive.hcatalog.messaging.*;
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

    @Override
    public CreateFunctionMessage getCreateFunctionMessage(String messageBody) {
        return null;
    }

    @Override
    public DropFunctionMessage getDropFunctionMessage(String messageBody) {
        return null;
    }

    @Override
    public CreateIndexMessage getCreateIndexMessage(String messageBody) {
        return null;
    }

    @Override
    public DropIndexMessage getDropIndexMessage(String messageBody) {
        return null;
    }

    @Override
    public AlterIndexMessage getAlterIndexMessage(String messageBody) {
        return null;
    }

    @Override
    public InsertMessage getInsertMessage(String messageBody) {
        try {
            return (JSONInsertMessage)mapper.readValue(messageBody, JSONInsertMessage.class);
        } catch (Exception var3) {
            throw new IllegalArgumentException("Could not construct JSONInsertMessage.", var3);
        }
    }

    static {
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static String serialize(Object object) {
        try {
            return mapper.writeValueAsString(object);
        }
        catch (Exception exception) {
            throw new IllegalArgumentException("Could not serialize: ", exception);
        }
    }
}
