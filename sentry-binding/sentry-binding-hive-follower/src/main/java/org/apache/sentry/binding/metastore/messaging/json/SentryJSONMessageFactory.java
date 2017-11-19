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

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.messaging.*;
import org.apache.hive.hcatalog.messaging.json.JSONAlterIndexMessage;
import org.apache.hive.hcatalog.messaging.json.JSONCreateFunctionMessage;
import org.apache.hive.hcatalog.messaging.json.JSONCreateIndexMessage;
import org.apache.hive.hcatalog.messaging.json.JSONDropFunctionMessage;
import org.apache.hive.hcatalog.messaging.json.JSONDropIndexMessage;
import org.apache.hive.hcatalog.messaging.json.JSONInsertMessage;

import java.util.*;

public class SentryJSONMessageFactory extends MessageFactory {
  private static final Log LOG = LogFactory.getLog(SentryJSONMessageFactory.class.getName());
  private static SentryJSONMessageDeserializer deserializer = new SentryJSONMessageDeserializer();

  // This class has basic information from a Partition. It is used to get a list of Partition
  // information from an iterator instead of having a list of Partition objects. Partition objects
  // may hold more information that can cause memory issues if we get a large list.
  private class PartitionBasicInfo {
    private List<Map<String, String>> partitionList = Lists.newLinkedList();
    private List<String> locations = Lists.newArrayList();

    public List<Map<String, String>> getPartitionList() {
      return partitionList;
    }

    public List<String> getLocations() {
      return locations;
    }
  }

  public SentryJSONMessageFactory() {
    LOG.info("Using SentryJSONMessageFactory for building Notification log messages ");
  }

  public MessageDeserializer getDeserializer() {
    return deserializer;
  }

  public String getVersion() {
    return "0.1";
  }

  public String getMessageFormat() {
    return "json";
  }

  public SentryJSONCreateDatabaseMessage buildCreateDatabaseMessage(Database db) {
    return new SentryJSONCreateDatabaseMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, db.getName(),
        now(), db.getLocationUri());
  }

  public SentryJSONDropDatabaseMessage buildDropDatabaseMessage(Database db) {
    return new SentryJSONDropDatabaseMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, db.getName(),
        now(), db.getLocationUri());
  }

  public SentryJSONCreateTableMessage buildCreateTableMessage(Table table) {
    return new SentryJSONCreateTableMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
        table.getTableName(), now(), table.getSd().getLocation());
  }

  public SentryJSONAlterTableMessage buildAlterTableMessage(Table before, Table after) {
    return new SentryJSONAlterTableMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, before.getDbName(),
        before.getTableName(), now(), before.getSd().getLocation(), after.getSd().getLocation());
  }

  public SentryJSONDropTableMessage buildDropTableMessage(Table table) {
    return new SentryJSONDropTableMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
        table.getTableName(), now(), table.getSd().getLocation());
  }

  @Override
  public SentryJSONAlterPartitionMessage buildAlterPartitionMessage(Table table,
      Partition before, Partition after) {
    return new SentryJSONAlterPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL,
        before.getDbName(), before.getTableName(), getPartitionKeyValues(table, before),
        after.getValues(), now(), before.getSd().getLocation(), after.getSd().getLocation());
  }

  @Override
  public DropPartitionMessage buildDropPartitionMessage(Table table, Iterator<Partition> partitions) {
    PartitionBasicInfo partitionBasicInfo = getPartitionBasicInfo(table, partitions);

    return new SentryJSONDropPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL,
        table.getDbName(), table.getTableName(), partitionBasicInfo.getPartitionList(),
        now(), partitionBasicInfo.getLocations());
  }

  @Override
  public CreateFunctionMessage buildCreateFunctionMessage(Function function) {
    // Sentry would be not be interested in CreateFunctionMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for CreateFunctionMessage
    return new JSONCreateFunctionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, function, now());
  }

  @Override
  public DropFunctionMessage buildDropFunctionMessage(Function function) {
    // Sentry would be not be interested in DropFunctionMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for DropFunctionMessage
    return new JSONDropFunctionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, function, now());

  }

  @Override
  public CreateIndexMessage buildCreateIndexMessage(Index index) {
    // Sentry would be not be interested in CreateIndexMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for CreateIndexMessage
    return new JSONCreateIndexMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, index, now());
  }

  @Override
  public DropIndexMessage buildDropIndexMessage(Index index) {
    // Sentry would be not be interested in DropIndexMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for DropIndexMessage
    return new JSONDropIndexMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, index, now());
  }

  @Override
  public AlterIndexMessage buildAlterIndexMessage(Index before, Index after) {
    // Sentry would be not be interested in AlterIndexMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for AlterIndexMessage
    return new JSONAlterIndexMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, before, after, now());
  }

  @Override
  public InsertMessage buildInsertMessage(String db, String table, Map<String,String> partKeyVals,
      List<String> files) {
    // Sentry would be not be interested in InsertMessage as these are generated when is data is
    // added inserted. This method is implemented for completeness. This is reason why, new sentry
    // JSON class is not defined for InsertMessage.
    return new JSONInsertMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, db, table, partKeyVals,
        files, now());
  }

  @Override
  public AddPartitionMessage buildAddPartitionMessage(Table table,
      Iterator<Partition> partitionsIterator) {
    PartitionBasicInfo partitionBasicInfo = getPartitionBasicInfo(table, partitionsIterator);

    return new SentryJSONAddPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
        table.getTableName(), partitionBasicInfo.getPartitionList(), now(),
        partitionBasicInfo.getLocations());
  }

  public AddPartitionMessage buildAddPartitionMessage(Table table,
                                                      List<Partition> partitions) {
    return buildAddPartitionMessage (table, partitions.iterator());
  }

  private PartitionBasicInfo getPartitionBasicInfo(Table table, Iterator<Partition> iterator) {
    PartitionBasicInfo partitionBasicInfo = new PartitionBasicInfo();
    while(iterator.hasNext()) {
      Partition partition = iterator.next();

      partitionBasicInfo.getPartitionList().add(getPartitionKeyValues(table, partition));
      partitionBasicInfo.getLocations().add(partition.getSd().getLocation());
    }

    return partitionBasicInfo;
  }

  private static Map<String, String> getPartitionKeyValues(Table table, Partition partition) {
    LinkedHashMap partitionKeys = new LinkedHashMap();

    for (int i = 0; i < table.getPartitionKeysSize(); ++i) {
      partitionKeys.put((table.getPartitionKeys().get(i)).getName(), partition.getValues().get(i));
    }

    return partitionKeys;
  }

  //This is private in parent class
  private long now() {
    return System.currentTimeMillis() / 1000L;
  }
}
