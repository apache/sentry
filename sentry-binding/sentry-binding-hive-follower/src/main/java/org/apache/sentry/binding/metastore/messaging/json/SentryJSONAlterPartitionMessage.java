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

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterPartitionMessage;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

public class SentryJSONAlterPartitionMessage extends JSONAlterPartitionMessage {
  @JsonProperty
  private String newLocation;
  @JsonProperty
  private String oldLocation;
  @JsonProperty
  private List<String> newValues;

  public SentryJSONAlterPartitionMessage() {
  }

  public SentryJSONAlterPartitionMessage(String server, String servicePrincipal, Table tableObj,
      Partition partitionObjBefore, Partition partitionObjAfter, Long timestamp) {
    this(server, servicePrincipal, tableObj, partitionObjBefore, partitionObjAfter, timestamp,
        partitionObjBefore.getSd().getLocation(), partitionObjAfter.getSd().getLocation(),
        partitionObjAfter.getValues());
  }

  public SentryJSONAlterPartitionMessage(String server, String servicePrincipal, Table tableObj,
      Partition partitionObjBefore, Partition partitionObjAfter, Long timestamp, String oldLocation,
      String newLocation, List<String> newValues) {
    super(server, servicePrincipal, tableObj, partitionObjBefore, partitionObjAfter, timestamp);
    this.newLocation = newLocation;
    this.oldLocation = oldLocation;
    this.newValues = newValues;
  }

  public String getNewLocation() {
    return newLocation;
  }

  public String getOldLocation() {
    return oldLocation;
  }

  public List<String> getNewValues() {
    return newValues;
  }

  @Override
  public String toString() {
    return SentryJSONMessageDeserializer.serialize(this);
  }
}
