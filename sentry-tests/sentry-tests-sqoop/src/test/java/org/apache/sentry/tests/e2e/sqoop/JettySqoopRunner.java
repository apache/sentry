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
package org.apache.sentry.tests.e2e.sqoop;

import static org.junit.Assert.assertEquals;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.test.db.DatabaseProvider;
import org.apache.sqoop.common.test.db.DatabaseProviderFactory;
import org.apache.sqoop.common.test.db.TableName;
import org.apache.sqoop.common.test.utils.NetworkUtils;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.server.SqoopJettyServer;
import org.apache.sqoop.test.minicluster.SqoopMiniCluster;
import org.apache.sqoop.test.utils.SqoopUtils;
import org.apache.sqoop.validation.Status;

import com.google.common.base.Joiner;

public class JettySqoopRunner {
  private SqoopServerEnableSentry server;
  private DatabaseProvider provider;
  private String temporaryPath;

  public JettySqoopRunner(String temporaryPath, String serverName, String sentrySite)
      throws Exception {
    this.temporaryPath = temporaryPath;
    this.server = new SqoopServerEnableSentry(temporaryPath, serverName, sentrySite);
    this.provider = DatabaseProviderFactory.getProvider(System.getProperties());
  }

  public void start() throws Exception {
    server.start();
    provider.start();
  }

  public void stop() throws Exception {
    server.stop();
    provider.stop();
  }

  /**
   * save link.
   *
   * With asserts to make sure that it was created correctly.
   * @param sqoopClient
   * @param link
   */
  public void saveLink(SqoopClient client, MLink link) {
    assertEquals(Status.OK, client.saveLink(link));
  }

  /**
   * update link.
   *
   * With asserts to make sure that it was created correctly.
   * @param sqoopClient
   * @param link
   * @param oldLinkName
   */
  public void updateLink(SqoopClient client, MLink link, String oldLinkName) {
    assertEquals(Status.OK, client.updateLink(link, oldLinkName));
  }


  /**
   * save job.
   *
   * With asserts to make sure that it was created correctly.
   *
   * @param job
   */
  public void saveJob(SqoopClient client, MJob job) {
    assertEquals(Status.OK, client.saveJob(job));
  }

  /**
   * fill link.
   *
   * With asserts to make sure that it was filled correctly.
   *
   * @param link
   */
  public void fillHdfsLink(MLink link) {
    MConfigList configs = link.getConnectorLinkConfig();
    configs.getStringInput("linkConfig.confDir").setValue(server.getConfigurationPath());
  }

  /**
   * Fill link config based on currently active provider.
   *
   * @param link MLink object to fill
   */
  public void fillRdbmsLinkConfig(MLink link) {
    MConfigList configs = link.getConnectorLinkConfig();
    configs.getStringInput("linkConfig.jdbcDriver").setValue(provider.getJdbcDriver());
    configs.getStringInput("linkConfig.connectionString").setValue(provider.getConnectionUrl());
    configs.getStringInput("linkConfig.username").setValue(provider.getConnectionUsername());
    configs.getStringInput("linkConfig.password").setValue(provider.getConnectionPassword());
  }

  public void fillHdfsFromConfig(MJob job) {
    MConfigList fromConfig = job.getFromJobConfig();
    fromConfig.getStringInput("fromJobConfig.inputDirectory").setValue(temporaryPath + "/output");
  }

  public void fillRdbmsToConfig(MJob job) {
    MConfigList toConfig = job.getToJobConfig();
    toConfig.getStringInput("toJobConfig.tableName").setValue(provider.
        escapeTableName(new TableName(getClass().getSimpleName()).getTableName()));
  }

  /**
   * get a sqoopClient for specific user
   * @param user
   */
  public SqoopClient getSqoopClient(String user) {
    setAuthenticationUser(user);
    return new SqoopClient(server.getServerUrl());
  }

  /**
   * Set the mock user in the Sqoop simple authentication
   * @param user
   */
  private void setAuthenticationUser(String user) {
    System.setProperty("user.name", user);
  }

  private static class SqoopServerEnableSentry extends SqoopMiniCluster {
      private Integer port;
      private String sentrySite;
      private String serverName;
      private SqoopJettyServer sqoopJettyServer;

      SqoopServerEnableSentry(String temporaryPath, String serverName, String sentrySite)
          throws Exception {
        super(temporaryPath);
        this.serverName = serverName;
        this.sentrySite = sentrySite;
        // Random port
        this.port = NetworkUtils.findAvailablePort();
      }

      @Override
      public Map<String, String> getSecurityConfiguration() {
        Map<String, String> properties = new HashMap<String, String>();
        configureAuthentication(properties);
        configureSentryAuthorization(properties);
        properties.put("org.apache.sqoop.jetty.port", port.toString());
        return properties;
      }

      private void configureAuthentication(Map<String, String> properties) {
        properties.put("org.apache.sqoop.authentication.type", "SIMPLE");
        properties.put("org.apache.sqoop.authentication.handler",
            "org.apache.sqoop.security.SimpleAuthenticationHandler");
      }

      private void configureSentryAuthorization(Map<String, String> properties) {
        properties.put("org.apache.sqoop.security.authorization.handler",
            "org.apache.sentry.sqoop.authz.SentryAuthorizationHandler");
        properties.put("org.apache.sqoop.security.authorization.access_controller",
            "org.apache.sentry.sqoop.authz.SentryAccessController");
        properties.put("org.apache.sqoop.security.authorization.validator",
            "org.apache.sentry.sqoop.authz.SentryAuthorizationValidator");
        properties.put("org.apache.sqoop.security.authorization.server_name", serverName);
        properties.put("sentry.sqoop.site.url", sentrySite);
        List<String> extraClassPath = new LinkedList<String>();
        for (String jar : System.getProperty("java.class.path").split(":")) {
          if ((jar.contains("sentry") || jar.contains("shiro-core") || jar.contains("libthrift"))
              && jar.endsWith("jar")) {
            extraClassPath.add(jar);
          }
        }
        properties.put("org.apache.sqoop.classpath.extra", Joiner.on(":").join(extraClassPath));
      }

      @Override
      public void start() throws Exception {
        prepareTemporaryPath();
        sqoopJettyServer = new SqoopJettyServer();
        sqoopJettyServer.startServer();
      }

      /** {@inheritDoc} */
      @Override
      public void stop() throws Exception {
        if (sqoopJettyServer != null) {
          sqoopJettyServer.stopServerForTest();
        }
      }

      /**
      * Return server URL.
      */
      @Override
      public String getServerUrl() {
        if (sqoopJettyServer != null) {
          String serverUrl = sqoopJettyServer.getServerUrl();
          // Replace the hostname of server url with FQDN
          String host;
          try {
            host = new URL(serverUrl).getHost();
          } catch (MalformedURLException e) {
            throw new RuntimeException("Invalid sqoop server url: " + serverUrl);
          }

          String fqdn = SqoopUtils.getLocalHostName();
          return serverUrl.replaceFirst(host, fqdn);
        }
        throw new RuntimeException("Jetty server wasn't started.");
      }
  }

}
