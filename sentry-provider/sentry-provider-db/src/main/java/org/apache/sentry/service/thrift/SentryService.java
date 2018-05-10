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

package org.apache.sentry.service.thrift;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.EventListener;
import java.util.List;
import java.util.concurrent.*;

import javax.security.auth.Subject;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.sentry.Command;
import org.apache.sentry.api.common.SentryServiceUtil;
import org.apache.sentry.core.common.utils.SigUtils;
import org.apache.sentry.provider.db.service.persistent.HMSFollower;
import org.apache.sentry.provider.db.service.persistent.LeaderStatusMonitor;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.api.service.thrift.SentryHealthCheckServletContextListener;
import org.apache.sentry.api.service.thrift.SentryMetrics;
import org.apache.sentry.api.service.thrift.SentryMetricsServletContextListener;
import org.apache.sentry.api.service.thrift.SentryWebServer;
import org.apache.sentry.service.common.ServiceConstants;
import org.apache.sentry.service.common.ServiceConstants.ConfUtilties;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.eclipse.jetty.util.MultiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import static org.apache.sentry.core.common.utils.SigUtils.registerSigListener;

// Enable signal handler for HA leader/follower status if configured
public class SentryService implements Callable, SigUtils.SigListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryService.class);
  private HiveSimpleConnectionFactory hiveConnectionFactory;

  private static final String SENTRY_SERVICE_THREAD_NAME = "sentry-service";
  private static final String HMSFOLLOWER_THREAD_NAME = "hms-follower";
  private static final String STORE_CLEANER_THREAD_NAME = "store-cleaner";
  private static final String SERVICE_SHUTDOWN_THREAD_NAME = "service-shutdown";

  private enum Status {
    NOT_STARTED,
    STARTED,
  }

  private final Configuration conf;
  private final InetSocketAddress address;
  private final int maxThreads;
  private final int minThreads;
  private final boolean kerberos;
  private final String principal;
  private final String[] principalParts;
  private final String keytab;
  private final ExecutorService serviceExecutor;
  private ScheduledExecutorService hmsFollowerExecutor = null;
  private HMSFollower hmsFollower = null;
  private Future serviceStatus;
  private TServer thriftServer;
  private Status status;
  private final int webServerPort;
  private SentryWebServer sentryWebServer;
  private final long maxMessageSize;
  /*
    sentryStore provides the data access for sentry data. It is the singleton instance shared
    between various {@link SentryPolicyService}, i.e., {@link SentryPolicyStoreProcessor} and
    {@link HMSFollower}.
   */
  private final SentryStore sentryStore;
  private ScheduledExecutorService sentryStoreCleanService;
  private final LeaderStatusMonitor leaderMonitor;

  public SentryService(Configuration conf) throws Exception {
    this.conf = conf;
    int port = conf
        .getInt(ServerConfig.RPC_PORT, ServerConfig.RPC_PORT_DEFAULT);
    if (port == 0) {
      port = findFreePort();
      conf.setInt(ServerConfig.RPC_PORT, port);
    }
    this.address = NetUtils.createSocketAddr(
        conf.get(ServerConfig.RPC_ADDRESS, ServerConfig.RPC_ADDRESS_DEFAULT),
        port);
    LOGGER.info("Configured on address {}", address);
    kerberos = ServerConfig.SECURITY_MODE_KERBEROS.equalsIgnoreCase(
        conf.get(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_KERBEROS).trim());
    maxThreads = conf.getInt(ServerConfig.RPC_MAX_THREADS,
        ServerConfig.RPC_MAX_THREADS_DEFAULT);
    minThreads = conf.getInt(ServerConfig.RPC_MIN_THREADS,
        ServerConfig.RPC_MIN_THREADS_DEFAULT);
    maxMessageSize = conf.getLong(ServerConfig.SENTRY_POLICY_SERVER_THRIFT_MAX_MESSAGE_SIZE,
        ServerConfig.SENTRY_POLICY_SERVER_THRIFT_MAX_MESSAGE_SIZE_DEFAULT);
    if (kerberos) {
      // Use Hadoop libraries to translate the _HOST placeholder with actual hostname
      try {
        String rawPrincipal = Preconditions.checkNotNull(conf.get(ServerConfig.PRINCIPAL), ServerConfig.PRINCIPAL + " is required");
        principal = SecurityUtil.getServerPrincipal(rawPrincipal, address.getAddress());
      } catch(IOException io) {
        throw new RuntimeException("Can't translate kerberos principal'", io);
      }
      LOGGER.info("Using kerberos principal: {}", principal);

      principalParts = SaslRpcServer.splitKerberosName(principal);
      Preconditions.checkArgument(principalParts.length == 3,
          "Kerberos principal should have 3 parts: " + principal);
      keytab = Preconditions.checkNotNull(conf.get(ServerConfig.KEY_TAB),
          ServerConfig.KEY_TAB + " is required");
      File keytabFile = new File(keytab);
      Preconditions.checkState(keytabFile.isFile() && keytabFile.canRead(),
          "Keytab %s does not exist or is not readable.", keytab);
    } else {
      principal = null;
      principalParts = null;
      keytab = null;
    }
    ThreadFactory sentryServiceThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat(SENTRY_SERVICE_THREAD_NAME)
        .build();
    serviceExecutor = Executors.newSingleThreadExecutor(sentryServiceThreadFactory);
    this.sentryStore = new SentryStore(conf);
    sentryStore.setPersistUpdateDeltas(SentryServiceUtil.isHDFSSyncEnabled(conf));
    this.leaderMonitor = LeaderStatusMonitor.getLeaderStatusMonitor(conf);
    webServerPort = conf.getInt(ServerConfig.SENTRY_WEB_PORT, ServerConfig.SENTRY_WEB_PORT_DEFAULT);

    status = Status.NOT_STARTED;

    // Enable signal handler for HA leader/follower status if configured
    String sigName = conf.get(ServerConfig.SERVER_HA_STANDBY_SIG);
    if ((sigName != null) && !sigName.isEmpty()) {
      LOGGER.info("Registering signal handler {} for HA", sigName);
      try {
        registerSigListener(sigName, this);
      } catch (Exception e) {
        LOGGER.error("Failed to register signal", e);
      }
    }
  }

  @Override
  public String call() throws Exception {
    SentryKerberosContext kerberosContext = null;
    try {
      status = Status.STARTED;
      if (kerberos) {
        kerberosContext = new SentryKerberosContext(principal, keytab, true);
        Subject.doAs(kerberosContext.getSubject(), new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            runServer();
            return null;
          }
        });
      } else {
        runServer();
      }
    } catch (Exception t) {
      LOGGER.error("Error starting server", t);
      throw new Exception("Error starting server", t);
    } finally {
      if (kerberosContext != null) {
        kerberosContext.shutDown();
      }
      status = Status.NOT_STARTED;
    }
    return null;
  }

  private void runServer() throws Exception {

    startSentryStoreCleaner(conf);
    startHMSFollower(conf);

    Iterable<String> processorFactories = ConfUtilties.CLASS_SPLITTER
        .split(conf.get(ServerConfig.PROCESSOR_FACTORIES,
            ServerConfig.PROCESSOR_FACTORIES_DEFAULT).trim());
    TMultiplexedProcessor processor = new TMultiplexedProcessor();
    boolean registeredProcessor = false;
    for (String processorFactory : processorFactories) {
      Class<?> clazz = conf.getClassByName(processorFactory);
      if (!ProcessorFactory.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException("Processor Factory "
            + processorFactory + " is not a "
            + ProcessorFactory.class.getName());
      }
      try {
        Constructor<?> constructor = clazz
            .getConstructor(Configuration.class);
        LOGGER.info("ProcessorFactory being used: " + clazz.getCanonicalName());
        ProcessorFactory factory = (ProcessorFactory) constructor
            .newInstance(conf);
        boolean registerStatus = factory.register(processor, sentryStore);
        if (!registerStatus) {
          LOGGER.error("Failed to register " + clazz.getCanonicalName());
        }
        registeredProcessor = registerStatus || registeredProcessor;
      } catch (Exception e) {
        throw new IllegalStateException("Could not create "
            + processorFactory, e);
      }
    }
    if (!registeredProcessor) {
      throw new IllegalStateException(
          "Failed to register any processors from " + processorFactories);
    }
    addSentryServiceGauge();
    TServerTransport serverTransport = new TServerSocket(address);
    TTransportFactory transportFactory = null;
    if (kerberos) {
      TSaslServerTransport.Factory saslTransportFactory = new TSaslServerTransport.Factory();
      saslTransportFactory.addServerDefinition(AuthMethod.KERBEROS
          .getMechanismName(), principalParts[0], principalParts[1],
              ServerConfig.SASL_PROPERTIES, new GSSCallback(conf));
      transportFactory = saslTransportFactory;
    } else {
      transportFactory = new TTransportFactory();
    }
    TThreadPoolServer.Args args = new TThreadPoolServer.Args(
        serverTransport).processor(processor)
        .transportFactory(transportFactory)
        .protocolFactory(new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize))
        .minWorkerThreads(minThreads).maxWorkerThreads(maxThreads);
    thriftServer = new TThreadPoolServer(args);
    LOGGER.info("Serving on {}", address);
    startSentryWebServer();

    // thriftServer.serve() does not return until thriftServer is stopped. Need to log before
    // calling thriftServer.serve()
    LOGGER.info("Sentry service is ready to serve client requests");

    // Allow clients/users watching the console to know when sentry is ready
    System.out.println("Sentry service is ready to serve client requests");
    SentryStateBank.enableState(SentryServiceState.COMPONENT, SentryServiceState.SERVICE_RUNNING);
    thriftServer.serve();
  }

  private void startHMSFollower(Configuration conf) throws Exception {
    boolean syncPolicyStore = SentryServiceUtil.isSyncPolicyStoreEnabled(conf);

    if ((!SentryServiceUtil.isHDFSSyncEnabled(conf)) && (!syncPolicyStore)) {
      LOGGER.info("HMS follower is not started because HDFS sync is disabled and perm sync is disabled");
      return;
    }

    String metastoreURI = SentryServiceUtil.getHiveMetastoreURI();
    if (metastoreURI == null) {
      LOGGER.info("Metastore uri is not configured. Do not start HMSFollower");
      return;
    }

    LOGGER.info("Starting HMSFollower to HMS {}", metastoreURI);

    Preconditions.checkState(hmsFollower == null);
    Preconditions.checkState(hmsFollowerExecutor == null);
    Preconditions.checkState(hiveConnectionFactory == null);

    hiveConnectionFactory = new HiveSimpleConnectionFactory(conf, new HiveConf());
    hiveConnectionFactory.init();
    hmsFollower = new HMSFollower(conf, sentryStore, leaderMonitor, hiveConnectionFactory);
    long initDelay = conf.getLong(ServerConfig.SENTRY_HMSFOLLOWER_INIT_DELAY_MILLS,
            ServerConfig.SENTRY_HMSFOLLOWER_INIT_DELAY_MILLS_DEFAULT);
    long period = conf.getLong(ServerConfig.SENTRY_HMSFOLLOWER_INTERVAL_MILLS,
            ServerConfig.SENTRY_HMSFOLLOWER_INTERVAL_MILLS_DEFAULT);
    try {
      ThreadFactory hmsFollowerThreadFactory = new ThreadFactoryBuilder()
          .setNameFormat(HMSFOLLOWER_THREAD_NAME)
          .build();
      hmsFollowerExecutor = Executors.newScheduledThreadPool(1, hmsFollowerThreadFactory);
      hmsFollowerExecutor.scheduleAtFixedRate(hmsFollower,
              initDelay, period, TimeUnit.MILLISECONDS);
    } catch (IllegalArgumentException e) {
      LOGGER.error(String.format("Could not start HMSFollower due to illegal argument. period is %s ms",
              period), e);
      throw e;
    }
  }

  private void stopHMSFollower(Configuration conf) {
    if ((hmsFollowerExecutor == null) || (hmsFollower == null)) {
        Preconditions.checkState(hmsFollower == null);
        Preconditions.checkState(hmsFollowerExecutor == null);

        LOGGER.debug("Skip shuting down hmsFollowerExecutor and closing hmsFollower because they are not created");
        return;
    }

    Preconditions.checkNotNull(hmsFollowerExecutor);
    Preconditions.checkNotNull(hmsFollower);
    Preconditions.checkNotNull(hiveConnectionFactory);

    // use follower scheduling interval as timeout for shutting down its executor as
    // such scheduling interval should be an upper bound of how long the task normally takes to finish
    long timeoutValue = conf.getLong(ServerConfig.SENTRY_HMSFOLLOWER_INTERVAL_MILLS,
            ServerConfig.SENTRY_HMSFOLLOWER_INTERVAL_MILLS_DEFAULT);
    try {
      SentryServiceUtil.shutdownAndAwaitTermination(hmsFollowerExecutor, "hmsFollowerExecutor",
              timeoutValue, TimeUnit.MILLISECONDS, LOGGER);
    } finally {
      try {
        hiveConnectionFactory.close();
      } catch (Exception e) {
        LOGGER.error("Can't close HiveConnectionFactory", e);
      }
      hmsFollowerExecutor = null;
      hiveConnectionFactory = null;
      try {
        // close connections
        hmsFollower.close();
      } catch (Exception ex) {
        LOGGER.error("HMSFollower.close() failed", ex);
      } finally {
        hmsFollower = null;
      }
    }
  }

  private void startSentryStoreCleaner(Configuration conf) {
    Preconditions.checkState(sentryStoreCleanService == null);

    // If SENTRY_STORE_CLEAN_PERIOD_SECONDS is set to positive, the background SentryStore cleaning
    // thread is enabled. Currently, it only purges the delta changes {@link MSentryChange} in
    // the sentry store.
    long storeCleanPeriodSecs = conf.getLong(
            ServerConfig.SENTRY_STORE_CLEAN_PERIOD_SECONDS,
            ServerConfig.SENTRY_STORE_CLEAN_PERIOD_SECONDS_DEFAULT);
    if (storeCleanPeriodSecs <= 0) {
      return;
    }

    try {
      Runnable storeCleaner = new Runnable() {
        @Override
        public void run() {
          if (leaderMonitor.isLeader()) {
            sentryStore.purgeDeltaChangeTables();
            sentryStore.purgeNotificationIdTable();
          }
        }
      };

      ThreadFactory sentryStoreCleanerThreadFactory = new ThreadFactoryBuilder()
          .setNameFormat(STORE_CLEANER_THREAD_NAME)
          .build();
      sentryStoreCleanService = Executors.newSingleThreadScheduledExecutor(sentryStoreCleanerThreadFactory);
      sentryStoreCleanService.scheduleWithFixedDelay(
              storeCleaner, 0, storeCleanPeriodSecs, TimeUnit.SECONDS);

      LOGGER.info("sentry store cleaner is scheduled with interval {} seconds", storeCleanPeriodSecs);
    }
    catch(IllegalArgumentException e){
      LOGGER.error("Could not start SentryStoreCleaner due to illegal argument", e);
      sentryStoreCleanService = null;
    }
  }

  private void stopSentryStoreCleaner() {
    Preconditions.checkNotNull(sentryStoreCleanService);

    try {
      SentryServiceUtil.shutdownAndAwaitTermination(sentryStoreCleanService, "sentryStoreCleanService",
              10, TimeUnit.SECONDS, LOGGER);
    }
    finally {
      sentryStoreCleanService = null;
    }
  }

  private void addSentryServiceGauge() {
    SentryMetrics.getInstance().addSentryServiceGauges(this);
  }

  private void startSentryWebServer() throws Exception{
    Boolean sentryReportingEnable = conf.getBoolean(ServerConfig.SENTRY_WEB_ENABLE,
        ServerConfig.SENTRY_WEB_ENABLE_DEFAULT);
    if(sentryReportingEnable) {
      List<EventListener> listenerList = new ArrayList<>();
      listenerList.add(new SentryHealthCheckServletContextListener());
      listenerList.add(new SentryMetricsServletContextListener());
      sentryWebServer = new SentryWebServer(listenerList, webServerPort, conf);
      sentryWebServer.start();
    }
  }

  private void stopSentryWebServer() throws Exception{
    if( sentryWebServer != null) {
      sentryWebServer.stop();
      sentryWebServer = null;
    }
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  public synchronized boolean isRunning() {
    return status == Status.STARTED && thriftServer != null
        && thriftServer.isServing();
  }

  public synchronized void start() throws Exception{
    if (status != Status.NOT_STARTED) {
      throw new IllegalStateException("Cannot start when " + status);
    }
    LOGGER.info("Attempting to start...");
    serviceStatus = serviceExecutor.submit(this);
  }

  public synchronized void stop() throws Exception{
    MultiException exception = null;
    LOGGER.info("Attempting to stop...");
    leaderMonitor.close();
    if (isRunning()) {
      LOGGER.info("Attempting to stop sentry thrift service...");
      try {
        thriftServer.stop();
        thriftServer = null;
        status = Status.NOT_STARTED;
      } catch (Exception e) {
        LOGGER.error("Error while stopping sentry thrift service", e);
        exception = addMultiException(exception,e);
      }
    } else {
      thriftServer = null;
      status = Status.NOT_STARTED;
      LOGGER.info("Sentry thrift service is already stopped...");
    }
    if (isWebServerRunning()) {
      try {
        LOGGER.info("Attempting to stop sentry web service...");
        stopSentryWebServer();
      } catch (Exception e) {
        LOGGER.error("Error while stopping sentry web service", e);
        exception = addMultiException(exception,e);
      }
    } else {
      LOGGER.info("Sentry web service is already stopped...");
    }

    stopHMSFollower(conf);
    stopSentryStoreCleaner();

    if (exception != null) {
      exception.ifExceptionThrow();
    }
    SentryStateBank.disableState(SentryServiceState.COMPONENT,SentryServiceState.SERVICE_RUNNING);
    LOGGER.info("Stopped...");
  }

  /**
   * If the current daemon is active, make it standby.
   * Here 'active' means it is the only daemon that can fetch snapshots from HMA and write
   * to the backend DB.
   */
  @VisibleForTesting
  public synchronized void becomeStandby() {
    leaderMonitor.deactivate();
  }

  private MultiException addMultiException(MultiException exception, Exception e) {
    MultiException newException = exception;
    if (newException == null) {
      newException = new MultiException();
    }
    newException.add(e);
    return newException;
  }

  private boolean isWebServerRunning() {
    return sentryWebServer != null
        && sentryWebServer.isAlive();
  }

  private static int findFreePort() {
    int attempts = 0;
    while (attempts++ <= 1000) {
      try {
        ServerSocket s = new ServerSocket(0);
        int port = s.getLocalPort();
        s.close();
        return port;
      } catch (IOException e) {
        // ignore and retry
      }
    }
    throw new IllegalStateException("Unable to find a port after 1000 attempts");
  }

  public static Configuration loadConfig(String configFileName)
      throws MalformedURLException {
    File configFile = null;
    if (configFileName == null) {
      throw new IllegalArgumentException("Usage: "
          + ServiceConstants.ServiceArgs.CONFIG_FILE_LONG
          + " path/to/sentry-service.xml");
    } else if (!((configFile = new File(configFileName)).isFile() && configFile
        .canRead())) {
      throw new IllegalArgumentException("Cannot read configuration file "
          + configFile);
    }
    Configuration conf = new Configuration(false);
    conf.addResource(configFile.toURI().toURL(), true);
    return conf;
  }

  public static class CommandImpl implements Command {
    @Override
    public void run(String[] args) throws Exception {
      CommandLineParser parser = new GnuParser();
      Options options = new Options();
      options.addOption(ServiceConstants.ServiceArgs.CONFIG_FILE_SHORT,
          ServiceConstants.ServiceArgs.CONFIG_FILE_LONG,
          true, "Sentry Service configuration file");
      CommandLine commandLine = parser.parse(options, args);
      String configFileName = commandLine.getOptionValue(ServiceConstants.
          ServiceArgs.CONFIG_FILE_LONG);
      File configFile = null;
      if (configFileName == null || commandLine.hasOption("h") || commandLine.hasOption("help")) {
        // print usage
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("sentry --command service", options);
        System.exit(-1);
      } else if(!((configFile = new File(configFileName)).isFile() && configFile.canRead())) {
        throw new IllegalArgumentException("Cannot read configuration file " + configFile);
      }
      Configuration serverConf = loadConfig(configFileName);
      final SentryService server = new SentryService(serverConf);
      server.start();

      ThreadFactory serviceShutdownThreadFactory = new ThreadFactoryBuilder()
          .setNameFormat(SERVICE_SHUTDOWN_THREAD_NAME)
          .build();
      Runtime.getRuntime().addShutdownHook(serviceShutdownThreadFactory.newThread(new Runnable() {
        @Override
        public void run() {
          LOGGER.info("ShutdownHook shutting down server");
          try {
            server.stop();
          } catch (Throwable t) {
            LOGGER.error("Error stopping SentryService", t);
            System.exit(1);
          }
        }
      }));

      // Let's wait on the service to stop
      try {
        // Wait for the service thread to finish
        server.serviceStatus.get();
      } finally {
        server.serviceExecutor.shutdown();
      }
    }
  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * Add Thrift event handler to underlying thrift threadpool server
   * @param eventHandler
   */
  public void setThriftEventHandler(TServerEventHandler eventHandler) throws IllegalStateException {
    if (thriftServer == null) {
      throw new IllegalStateException("Server is not initialized or stopped");
    }
    thriftServer.setServerEventHandler(eventHandler);
  }

  public TServerEventHandler getThriftEventHandler() throws IllegalStateException {
    if (thriftServer == null) {
      throw new IllegalStateException("Server is not initialized or stopped");
    }
    return thriftServer.getEventHandler();
  }

  public Gauge<Boolean> getIsActiveGauge() {
    return new Gauge<Boolean>() {
      @Override
      public Boolean getValue() {
        return leaderMonitor.isLeader();
      }
    };
  }

  public Gauge<Long> getBecomeActiveCount() {
    return new Gauge<Long>() {
      @Override
      public Long getValue() {
        return leaderMonitor.getLeaderCount();
      }
    };
  }

  @Override
  public void onSignal(String signalName) {
    // Become follower
    leaderMonitor.deactivate();
  }

  /**
   * Restart HMSFollower with new configuration
   * @param newConf Configuration
   * @throws Exception
   */
  @VisibleForTesting
  public void restartHMSFollower(Configuration newConf) throws Exception{
    stopHMSFollower(conf);
    startHMSFollower(newConf);
  }
}