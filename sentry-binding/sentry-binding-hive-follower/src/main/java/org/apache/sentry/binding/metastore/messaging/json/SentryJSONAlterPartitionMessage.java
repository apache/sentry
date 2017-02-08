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

import com.google.common.collect.ImmutableMap;
import org.apache.hive.hcatalog.messaging.json.JSONAlterPartitionMessage;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;

public class SentryJSONAlterPartitionMessage extends JSONAlterPartitionMessage {
    @JsonProperty
    private String newLocation;
    @JsonProperty
    private String oldLocation;
    @JsonProperty
    private Map<String, String> newKeyValues;

    public SentryJSONAlterPartitionMessage() {
        super("", "", "", "", ImmutableMap.<String, String>of(), null);
    }

    public SentryJSONAlterPartitionMessage(String server, String servicePrincipal,
                                           String db, String table,
                                           Map<String, String> values, Map<String, String> newValues,
                                           Long timestamp, String oldlocation,
                                           String newLocation) {
        super(server, servicePrincipal, db, table, values, timestamp);
        this.newLocation = newLocation;
        this.oldLocation = oldlocation;
        this.newKeyValues = newValues;
    }

    public String getNewLocation() {
        return newLocation;
    }

    public String getOldLocation() {
        return oldLocation;
    }

    public Map<String, String> getNewKeyValues() {
        return newKeyValues;
    }

    @Override
    public String toString() {
        return SentryJSONMessageDeserializer.serialize(this);
    }
}
