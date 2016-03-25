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
package org.apache.sentry.binding.hive.v2.metastore;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.sentry.binding.metastore.SentryMetastorePostEventListenerBase;
import org.apache.sentry.provider.db.SentryMetastoreListenerPlugin;

public class SentryMetastorePostEventListenerV2 extends SentryMetastorePostEventListenerBase {

  public SentryMetastorePostEventListenerV2(Configuration config) {
    super(config);
  }

  @Override
  public void onAddPartition(AddPartitionEvent partitionEvent)
      throws MetaException {
    if (partitionEvent != null && partitionEvent.getPartitionIterator() != null) {
      Iterator<Partition> it = partitionEvent.getPartitionIterator();
      while (it.hasNext()) {
        Partition part = it.next();
        if (part.getSd() != null && part.getSd().getLocation() != null) {
          String authzObj = part.getDbName() + "." + part.getTableName();
          String path = part.getSd().getLocation();
          for (SentryMetastoreListenerPlugin plugin : sentryPlugins) {
            plugin.addPath(authzObj, path);
          }
        }
      }
    }
  }

  @Override
  public void onDropPartition(DropPartitionEvent partitionEvent)
      throws MetaException {
    if (partitionEvent != null && partitionEvent.getPartitionIterator() != null) {
      String authzObj = partitionEvent.getTable().getDbName() + "."
          + partitionEvent.getTable().getTableName();
      Iterator<Partition> it = partitionEvent.getPartitionIterator();
      while (it.hasNext()) {
        Partition part = it.next();
        if (part.getSd() != null && part.getSd().getLocation() != null) {
          String path = part.getSd().getLocation();
          for (SentryMetastoreListenerPlugin plugin : sentryPlugins) {
            plugin.removePath(authzObj, path);
          }
        }
      }
    }
  }

}
