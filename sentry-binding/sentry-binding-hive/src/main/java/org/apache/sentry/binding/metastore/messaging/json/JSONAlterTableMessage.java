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

import org.apache.hive.hcatalog.messaging.AlterTableMessage;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * This class is required as this class does not have a default contructor in Hive 1.1.0
 */
public class JSONAlterTableMessage extends AlterTableMessage {
    @JsonProperty
    String server;
    @JsonProperty
    String servicePrincipal;
    @JsonProperty
    String db;
    @JsonProperty
    String table;
    @JsonProperty
    Long timestamp;

    public JSONAlterTableMessage() {}
    public JSONAlterTableMessage(String server, String servicePrincipal, String db, String table, Long timestamp) {
        this.server = server;
        this.servicePrincipal = servicePrincipal;
        this.db = db;
        this.table = table;
        this.timestamp = timestamp;
        this.checkValid();
    }

    public String getServer() {
        return this.server;
    }

    public String getServicePrincipal() {
        return this.servicePrincipal;
    }

    public String getDB() {
        return this.db;
    }

    public Long getTimestamp() {
        return this.timestamp;
    }

    public String getTable() {
        return this.table;
    }
}
