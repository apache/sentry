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
package org.apache.sentry.tests.e2e.hiveserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.google.common.base.Strings;

public abstract class AbstractHiveServer implements HiveServer {

  private static final String LINK_FAILURE_SQL_STATE = "08S01";

  private final Configuration configuration;
  private final String hostname;
  private final int port;

  public AbstractHiveServer(Configuration configuration, String hostname,
      int port) {
    this.configuration = configuration;
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  public String getProperty(String key) {
    return configuration.get(key);
  }

  @Override
  public String getURL() {
    return "jdbc:hive2://" + hostname + ":" + port + "/default";
  }

  public Connection createConnection(String user, String password) throws Exception{
    String url = getURL();
    Connection connection =  DriverManager.getConnection(url, user, password);
    return connection;
  }

  protected static String getHostname(HiveConf hiveConf) {
    return hiveConf.get(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.toString(), "localhost").trim();
  }
  protected static int getPort(HiveConf hiveConf) {
    return Integer.parseInt(hiveConf.get(ConfVars.HIVE_SERVER2_THRIFT_PORT.toString(), "10000").trim());
  }
  protected static void waitForStartup(HiveServer hiveServer) throws Exception {
    int waitTime = 0;
    long startupTimeout = 1000L * 10L;
    do {
      Thread.sleep(500L);
      waitTime += 500L;
      if (waitTime > startupTimeout) {
        throw new TimeoutException("Couldn't access new HiveServer: " + hiveServer.getURL());
      }
      try {
        Connection connection =  DriverManager.getConnection(hiveServer.getURL(), "hive", "bar");
        connection.close();
        break;
      } catch (SQLException e) {
        String state = Strings.nullToEmpty(e.getSQLState()).trim();
        if (!state.equalsIgnoreCase(LINK_FAILURE_SQL_STATE)) {
          throw e;
        }
      }
    } while (true);
  }
}
