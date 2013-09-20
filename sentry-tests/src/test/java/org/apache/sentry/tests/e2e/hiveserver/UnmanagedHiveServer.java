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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class UnmanagedHiveServer implements HiveServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnmanagedHiveServer.class);
  public static String hostname;
  public static int port;
  public static final String hs2Host = System.getProperty("hs2Host");
  public static final int hs2Port = Integer.parseInt(System.getProperty("hivePort", "10000"));
  public static final String auth = System.getProperty("auth", "kerberos");
  public static final String hivePrincipal = System.getProperty("hivePrincipal");
  public static final String kerbRealm = System.getProperty("kerberosRealm");
  private HiveConf hiveConf;

  public UnmanagedHiveServer() {
    Preconditions.checkNotNull(hs2Host);
    if(auth.equalsIgnoreCase("kerberos")){
      Preconditions.checkNotNull(kerbRealm);
      Preconditions.checkNotNull(hivePrincipal);
    }
    this.hostname = hs2Host;
    this.port = hs2Port;
    hiveConf = new HiveConf();
  }

  @Override
  public void start() throws Exception {
    //For Unmanaged HiveServer, service need not be started within the test
  }

  @Override
  public void shutdown() throws Exception {
    //For Unmanaged HiveServer, service need not be stopped within the test
  }

  @Override
  public String getURL() {
    return "jdbc:hive2://" + hostname + ":" + port + "/default;";
  }

  @Override
  public String getProperty(String key) {
   return hiveConf.get(key);
  }

  @Override
  public Connection createConnection(String user, String password) throws Exception{
    String url = getURL();
    Properties oProps = new Properties();

    if(auth.equalsIgnoreCase("kerberos")){
      String commandFormat = "kinit -kt /cdep/keytabs/%s.keytab %s@" + kerbRealm;
      String command = String.format(commandFormat, user, user, user);
      Process proc = Runtime.getRuntime().exec(command);
      String status = (proc.waitFor()==0)?"passed":"failed";
      LOGGER.info(command + ": " + status);

      command = "kinit -R";
      proc = Runtime.getRuntime().exec(command);
      status = (proc.waitFor()==0)?"passed":"failed";
      LOGGER.info(command + ": " + status);

      url += "principal=" + hivePrincipal;
    }else{
      oProps.setProperty("user",user);
      oProps.setProperty("password",password);
    }
    LOGGER.info("url: " + url);
    return DriverManager.getConnection(url, oProps);
  }
}
