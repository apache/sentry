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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hive.hcatalog.messaging.*;
import org.apache.hive.hcatalog.messaging.json.JSONInsertMessage;

import java.util.*;

public class SentryJSONMessageFactory extends MessageFactory {
    private static final Log LOG = LogFactory.getLog(SentryJSONMessageFactory.class.getName());
    private static SentryJSONMessageDeserializer deserializer = new SentryJSONMessageDeserializer();
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
                Long.valueOf(this.now()), db.getLocationUri());
    }
    public SentryJSONDropDatabaseMessage buildDropDatabaseMessage(Database db) {
        return new SentryJSONDropDatabaseMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, db.getName(),
                Long.valueOf(this.now()), db.getLocationUri());
    }

    public SentryJSONCreateTableMessage buildCreateTableMessage(Table table) {
        return new SentryJSONCreateTableMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
                table.getTableName(), Long.valueOf(this.now()), table.getSd().getLocation());
    }

    public SentryJSONAlterTableMessage buildAlterTableMessage(Table before, Table after) {
        return new SentryJSONAlterTableMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, before.getDbName(),
                before.getTableName(), Long.valueOf(this.now()), before.getSd().getLocation(), after.getSd().getLocation());
    }

    public SentryJSONDropTableMessage buildDropTableMessage(Table table) {
        return new SentryJSONDropTableMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
                table.getTableName(), Long.valueOf(this.now()), table.getSd().getLocation());
    }

    public SentryJSONAddPartitionMessage buildAddPartitionMessage(Table table, List<Partition> partitions) {
        return new SentryJSONAddPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
                table.getTableName(), getPartitionKeyValues(table, partitions), Long.valueOf(this.now()),
                getPartitionLocations(partitions));
    }

    private List<String> getPartitionLocations(List<Partition> partitions) {
        List<String> paths = new ArrayList<String>();
        for(Partition partition:partitions) {
            paths.add(partition.getSd().getLocation());
        }
        return paths;
    }

    //TODO: Not sure what is this used for. Need to investigate
    private List<String> getPartitionLocations(PartitionSpecProxy partitionSpec) {
        Iterator<Partition> iterator = partitionSpec.getPartitionIterator();
        List<String> locations = new ArrayList<String>();
        while(iterator.hasNext()) {
            locations.add(iterator.next().getSd().getLocation());
        }
        return locations;
    }

    @InterfaceAudience.LimitedPrivate({"Hive"})
    @InterfaceStability.Evolving
    public SentryJSONAddPartitionMessage buildAddPartitionMessage(Table table, PartitionSpecProxy partitionSpec) {
        return new SentryJSONAddPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
                table.getTableName(), getPartitionKeyValues(table, partitionSpec), Long.valueOf(this.now()),
                getPartitionLocations(partitionSpec));
    }

    public SentryJSONAlterPartitionMessage buildAlterPartitionMessage(Table table, Partition oldPartition, Partition newPartition) {
        /*
     f (partitionEvent.getOldPartition() != null) {
      oldLoc = partitionEvent.getOldPartition().getSd().getLocation();
    }
    if (partitionEvent.getNewPartition() != null) {
      newLoc = partitionEvent.getNewPartition().getSd().getLocation();
    }

    if ((oldLoc != null) && (newLoc != null) && (!oldLoc.equals(newLoc))) {
      String authzObj =
              partitionEvent.getOldPartition().getDbName() + "."
                      + partitionEvent.getOldPartition().getTableName();
      for (SentryMetastoreListenerPlugin plugin : sentryPlugins) {
        plugin.renameAuthzObject(authzObj, oldLoc,
                authzObj, newLoc);
      }
    }
        * */
        return new SentryJSONAlterPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, table.getDbName(),
                table.getTableName(), getPartitionKeyValues(table, oldPartition), Long.valueOf(this.now()),
                oldPartition.getSd().getLocation(), newPartition.getSd().getLocation());
    }

    public SentryJSONDropPartitionMessage buildDropPartitionMessage(Table table, Partition partition) {
        return new SentryJSONDropPartitionMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, partition.getDbName(),
                partition.getTableName(), Arrays.asList(getPartitionKeyValues(table, partition)),
                Long.valueOf(this.now()), partition.getSd().getLocation());
    }

    @Override
    public InsertMessage buildInsertMessage(String db, String table, Map<String, String> partKeyVals, List<String> files) {
        return new JSONInsertMessage(HCAT_SERVER_URL, HCAT_SERVICE_PRINCIPAL, db, table, partKeyVals,
            files, now());
    }

    private static Map<String, String> getPartitionKeyValues(Table table, Partition partition) {
        LinkedHashMap partitionKeys = new LinkedHashMap();

        for(int i = 0; i < table.getPartitionKeysSize(); ++i) {
            partitionKeys.put(((FieldSchema)table.getPartitionKeys().get(i)).getName(), partition.getValues().get(i));
        }

        return partitionKeys;
    }

    private static List<Map<String, String>> getPartitionKeyValues(Table table, List<Partition> partitions) {
        ArrayList partitionList = new ArrayList(partitions.size());
        Iterator i$ = partitions.iterator();

        while(i$.hasNext()) {
            Partition partition = (Partition)i$.next();
            partitionList.add(getPartitionKeyValues(table, partition));
        }

        return partitionList;
    }

    @InterfaceAudience.LimitedPrivate({"Hive"})
    @InterfaceStability.Evolving
    private static List<Map<String, String>> getPartitionKeyValues(Table table, PartitionSpecProxy partitionSpec) {
        ArrayList partitionList = new ArrayList();
        PartitionSpecProxy.PartitionIterator iterator = partitionSpec.getPartitionIterator();

        while(iterator.hasNext()) {
            Partition partition = (Partition)iterator.next();
            partitionList.add(getPartitionKeyValues(table, partition));
        }

        return partitionList;
    }
    //This is private in parent class
    private long now() {
        return System.currentTimeMillis() / 1000L;
    }
}
