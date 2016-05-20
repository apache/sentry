/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.tests.e2e.hive.hiveserver;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.fest.reflect.core.Reflection;

public class EmbeddedHiveServer implements HiveServer {

  @Override
  public void start() {
    // Fix for ACCESS-148. Resets a static field
    // so the default database is created even
    // though is has been created before in this JVM
    Reflection.staticField("createDefaultDB")
    .ofType(boolean.class)
    .in(HiveMetaStore.HMSHandler.class)
    .set(false);
  }

  public Connection createConnection(String user, String password) throws Exception{
    String url = getURL();
    DriverManager.setLoginTimeout(30);
    Connection connection =  DriverManager.getConnection(url, user, password);
    return connection;
  }

  @Override
  public void shutdown() {

  }

  @Override
  public String getURL() {
    return "jdbc:hive2://";
  }

  @Override
  public String getProperty(String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getOrgWarehouseDir() {
    return (String)null;
  }
}
