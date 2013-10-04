/*
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

package org.apache.sentry.binding.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.core.model.db.AccessURI;
import org.apache.sentry.core.model.db.Database;
import org.apache.sentry.core.model.db.Table;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public class SentryOnFailureHookContextImpl implements SentryOnFailureHookContext {

  private final String command;
  private final Set<ReadEntity> inputs;
  private final Set<WriteEntity> outputs;
  private final HiveOperation hiveOp;
  private final String userName;
  private final String ipAddress;
  private final Database database;
  private final Table table;
  private final AccessURI udfURI;
  private final AccessURI partitionURI;
  private final AuthorizationException authException;
  private final Configuration conf;

  public SentryOnFailureHookContextImpl(String command,
      Set<ReadEntity> inputs, Set<WriteEntity> outputs, HiveOperation hiveOp,
      Database db, Table tab, AccessURI udfURI, AccessURI partitionURI,
      String userName, String ipAddress, AuthorizationException e,
      Configuration conf) {
    this.command = command;
    this.inputs = inputs;
    this.outputs = outputs;
    this.hiveOp = hiveOp;
    this.userName = userName;
    this.ipAddress = ipAddress;
    this.database = db;
    this.table = tab;
    this.udfURI = udfURI;
    this.partitionURI = partitionURI;
    this.authException = e;
    this.conf = conf;
  }

  @Override
  public String getCommand() {
    return command;
  }

  @Override
  public Set<ReadEntity> getInputs() {
    return inputs;
  }

  @Override
  public Set<WriteEntity> getOutputs() {
    return outputs;
  }

  @Override
  public HiveOperation getHiveOp() {
    return hiveOp;
  }

  @Override
  public String getUserName() {
    return userName;
  }

  @Override
  public String getIpAddress() {
    return ipAddress;
  }

  @Override
  public Database getDatabase() {
    return database;
  }

  @Override
  public Table getTable() {
    return table;
  }

  @Override
  public AccessURI getUdfURI() {
    return udfURI;
  }

  @Override
  public AccessURI getPartitionURI() {
    return partitionURI;
  }

  @Override
  public AuthorizationException getException() {
    return authException;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}