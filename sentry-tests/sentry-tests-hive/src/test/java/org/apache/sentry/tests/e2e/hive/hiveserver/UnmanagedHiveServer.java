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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class UnmanagedHiveServer implements HiveServer {
  private static final Logger LOGGER = LoggerFactory.getLogger(UnmanagedHiveServer.class);
  private static final String HS2_HOST = HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname;
  private static final String HS2_PORT = HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.varname;
  private static final String HS2_AUTH = HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.varname;
  private static final String HS2_PRINCIPAL = HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL.varname;
  private static final String KEYTAB_LOCATION = System.getProperty("sentry.e2e.hive.keytabs.location");
  private static final String AUTHENTICATION_TYPE = System.getProperty(HS2_AUTH, "kerberos");

  private String hostname;
  private String port;

  private String hivePrincipal;
  private HiveConf hiveConf;

  public UnmanagedHiveServer() {
    hiveConf = new HiveConf();
    hostname = getSystemAndConfigProperties(HS2_HOST, null);
    port = getSystemAndConfigProperties(HS2_PORT, "10000");
    if(AUTHENTICATION_TYPE.equalsIgnoreCase("kerberos")){
      hivePrincipal = getSystemAndConfigProperties(HS2_PRINCIPAL, null);
    }
  }

  private String getSystemAndConfigProperties(String hiveVar, String defaultVal){
    String val = hiveConf.get(hiveVar);
    if(val == null || val.trim().equals("")){
      LOGGER.warn(hiveVar + " not found in the client hive-site.xml");
      if(defaultVal == null) {
        val = System.getProperty(hiveVar);
      }else {
        val = System.getProperty(hiveVar, defaultVal);
      }
      Preconditions.checkNotNull(val, "Required system property missing: Provide it using -D"+ hiveVar);
      LOGGER.info("Using from system property" + hiveVar + " = " + val );
    }else {
      LOGGER.info("Using from hive-site.xml" + hiveVar + " = " + val );
    }
    return val;
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
    if(key.equalsIgnoreCase(HiveConf.ConfVars.METASTOREWAREHOUSE.varname)) {
      return "hdfs://" + getSystemAndConfigProperties(key, null); //UnManagedHiveServer returns the warehouse directory without hdfs://
    }
   return getSystemAndConfigProperties(key, null);
  }

  @Override
  public Connection createConnection(String user, String password) throws Exception{
    String url = getURL();
    Properties oProps = new Properties();

    if(AUTHENTICATION_TYPE.equalsIgnoreCase("kerberos")){
      kinit(user);
      url += "principal=" + hivePrincipal;
    }else{
      oProps.setProperty("user",user);
      oProps.setProperty("password",password);
    }
    LOGGER.info("url: " + url);
    return DriverManager.getConnection(url, oProps);
  }
  public void kinit(String user) throws Exception{
    UserGroupInformation.loginUserFromKeytab(user, KEYTAB_LOCATION + "/" + user + ".keytab");
  }
}
