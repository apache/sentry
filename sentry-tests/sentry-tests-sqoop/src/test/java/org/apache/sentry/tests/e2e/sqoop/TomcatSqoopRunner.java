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
import static org.junit.Assert.assertNotSame;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.common.test.db.DatabaseProvider;
import org.apache.sqoop.common.test.db.DatabaseProviderFactory;
import org.apache.sqoop.common.test.db.TableName;
import org.apache.sqoop.common.test.utils.NetworkUtils;
import org.apache.sqoop.model.MConfigList;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MPersistableEntity;
import org.apache.sqoop.test.minicluster.SqoopMiniCluster;
import org.apache.sqoop.validation.Status;
import org.codehaus.cargo.container.ContainerType;
import org.codehaus.cargo.container.InstalledLocalContainer;
import org.codehaus.cargo.container.configuration.ConfigurationType;
import org.codehaus.cargo.container.configuration.LocalConfiguration;
import org.codehaus.cargo.container.deployable.WAR;
import org.codehaus.cargo.container.installer.Installer;
import org.codehaus.cargo.container.installer.ZipURLInstaller;
import org.codehaus.cargo.container.property.GeneralPropertySet;
import org.codehaus.cargo.container.property.ServletPropertySet;
import org.codehaus.cargo.container.tomcat.TomcatPropertySet;
import org.codehaus.cargo.generic.DefaultContainerFactory;
import org.codehaus.cargo.generic.configuration.DefaultConfigurationFactory;

import com.google.common.base.Joiner;

public class TomcatSqoopRunner {
  private static final Logger LOG = Logger.getLogger(TomcatSqoopRunner.class);
  private SqoopServerEnableSentry server;
  private DatabaseProvider provider;
  private String temporaryPath;

  public TomcatSqoopRunner(String temporaryPath, String serverName, String sentrySite)
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
   * create link.
   *
   * With asserts to make sure that it was created correctly.
   * @param sqoopClient
   * @param link
   */
  public void saveLink(SqoopClient client, MLink link) {
    assertEquals(Status.OK, client.saveLink(link));
    assertNotSame(MPersistableEntity.PERSISTANCE_ID_DEFAULT, link.getPersistenceId());
  }

  /**
   * create link.
   *
   * With asserts to make sure that it was created correctly.
   * @param sqoopClient
   * @param link
   */
  public void updateLink(SqoopClient client, MLink link) {
    assertEquals(Status.OK, client.updateLink(link));
    assertNotSame(MPersistableEntity.PERSISTANCE_ID_DEFAULT, link.getPersistenceId());
  }

  /**
   * Create job.
   *
   * With asserts to make sure that it was created correctly.
   *
   * @param job
   */
  public void saveJob(SqoopClient client, MJob job) {
    assertEquals(Status.OK, client.saveJob(job));
    assertNotSame(MPersistableEntity.PERSISTANCE_ID_DEFAULT, job.getPersistenceId());
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
    private static final String WAR_PATH = "thirdparty/sqoop.war";
    private static final String TOMCAT_PATH = "thirdparty/apache-tomcat-6.0.36.zip";

    private InstalledLocalContainer container = null;
    private Integer port;
    private Integer ajpPort;
    private String sentrySite;
    private String serverName;

    SqoopServerEnableSentry(String temporaryPath, String serverName, String sentrySite)
        throws Exception {
      super(temporaryPath);
      this.serverName = serverName;
      this.sentrySite = sentrySite;
      // Random port
      this.port = NetworkUtils.findAvailablePort();
      this.ajpPort = NetworkUtils.findAvailablePort();
    }

    @Override
    public Map<String, String> getSecurityConfiguration() {
      Map<String, String> properties = new HashMap<String, String>();
      configureAuthentication(properties);
      configureSentryAuthorization(properties);
      return properties;
    }

    private void configureAuthentication(Map<String, String> properties) {
      /** Simple Authentication */
      properties.put("org.apache.sqoop.authentication.type", "SIMPLE");
      properties.put("org.apache.sqoop.authentication.handler",
          "org.apache.sqoop.security.SimpleAuthenticationHandler");
    }

    private void configureSentryAuthorization(Map<String, String> properties) {
      properties.put("org.apache.sqoop.security.authorization.handler",
          "org.apache.sentry.sqoop.authz.SentryAuthorizationHander");
      properties.put("org.apache.sqoop.security.authorization.access_controller",
          "org.apache.sentry.sqoop.authz.SentryAccessController");
      properties.put("org.apache.sqoop.security.authorization.validator",
          "org.apache.sentry.sqoop.authz.SentryAuthorizationValidator");
      properties.put("org.apache.sqoop.security.authorization.server_name", serverName);
      properties.put("sentry.sqoop.site.url", sentrySite);
      /** set Sentry related jars into classpath */
      List<String> extraClassPath = new LinkedList<String>();
      for (String jar : System.getProperty("java.class.path").split(":")) {
        if ((jar.contains("sentry") || jar.contains("shiro-core") || jar.contains("libthrift"))
            && jar.endsWith("jar")) {
          extraClassPath.add(jar);
        }
      }
      properties.put("org.apache.sqoop.classpath.extra",Joiner.on(":").join(extraClassPath));
    }

    @Override
    public void start() throws Exception {
      // Container has already been started
      if (container != null) {
        return;
      }
      prepareTemporaryPath();

      // Source: http://cargo.codehaus.org/Functional+testing
      String tomcatPath = getTemporaryPath() + "/tomcat";
      String extractPath = tomcatPath + "/extract";
      String confPath = tomcatPath + "/conf";

      Installer installer = new ZipURLInstaller(new File(TOMCAT_PATH).toURI().toURL(), null, extractPath);
      installer.install();

      LocalConfiguration configuration = (LocalConfiguration) new DefaultConfigurationFactory()
          .createConfiguration("tomcat6x", ContainerType.INSTALLED, ConfigurationType.STANDALONE,
              confPath);
      container = (InstalledLocalContainer) new DefaultContainerFactory().createContainer("tomcat6x",
          ContainerType.INSTALLED, configuration);

      // Set home to our installed tomcat instance
      container.setHome(installer.getHome());

      // Store tomcat logs into file as they are quite handy for debugging
      container.setOutput(getTemporaryPath() + "/log/tomcat.log");

      // Propagate system properties to the container
      Map<String, String> map = new HashMap<String, String>((Map) System.getProperties());
      container.setSystemProperties(map);

      // Propagate Hadoop jars to the container classpath
      // In real world, they would be installed manually by user
      List<String> extraClassPath = new LinkedList<String>();
      String[] classpath = System.getProperty("java.class.path").split(":");
      for (String jar : classpath) {
        if (jar.contains("hadoop-") || // Hadoop jars
            jar.contains("hive-") || // Hive jars
            jar.contains("commons-") || // Apache Commons libraries
            jar.contains("httpcore-") || // Apache Http Core libraries
            jar.contains("httpclient-") || // Apache Http Client libraries
            jar.contains("htrace-") || // htrace-core libraries, new added in
                                       // Hadoop 2.6.0
            jar.contains("zookeeper-") || // zookeeper libraries, new added in
                                          // Hadoop 2.6.0
            jar.contains("curator-") || // curator libraries, new added in Hadoop
                                        // 2.6.0
            jar.contains("log4j-") || // Log4j
            jar.contains("slf4j-") || // Slf4j
            jar.contains("jackson-") || // Jackson
            jar.contains("derby") || // Derby drivers
            jar.contains("avro-") || // Avro
            jar.contains("parquet-") || // Parquet
            jar.contains("mysql") || // MySQL JDBC driver
            jar.contains("postgre") || // PostgreSQL JDBC driver
            jar.contains("oracle") || // Oracle driver
            jar.contains("terajdbc") || // Teradata driver
            jar.contains("tdgs") || // Teradata driver
            jar.contains("nzjdbc") || // Netezza driver
            jar.contains("sqljdbc") || // Microsoft SQL Server driver
            jar.contains("libfb303") || // Facebook thrift lib
            jar.contains("datanucleus-") || // Data nucleus libs
            jar.contains("google") // Google libraries (guava, ...)
        ) {
          extraClassPath.add(jar);
        }
      }
      container.setExtraClasspath(extraClassPath.toArray(new String[extraClassPath.size()]));

      // Finally deploy Sqoop server war file
      configuration.addDeployable(new WAR(WAR_PATH));
      configuration.setProperty(ServletPropertySet.PORT, port.toString());
      configuration.setProperty(TomcatPropertySet.AJP_PORT, ajpPort.toString());
      //configuration.setProperty(GeneralPropertySet.JVMARGS, "\"-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8006\"");
      LOG.info("Tomcat extract path: " + extractPath);
      LOG.info("Tomcat home path: " + installer.getHome());
      LOG.info("Tomcat config home path: " + confPath);
      LOG.info("Starting tomcat server on port " + port);
      container.start();
    }

    @Override
    public void stop() throws Exception {
      if (container != null) {
        container.stop();
      }
    }

    /**
     * Return server URL.
     */
    public String getServerUrl() {
      // We're not doing any changes, so return default URL
      return "http://localhost:" + port + "/sqoop/";
    }
  }
}
