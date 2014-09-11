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

package org.apache.sentry.service.thrift;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import javax.security.auth.Subject;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.sentry.Command;
import org.apache.sentry.service.thrift.ServiceConstants.ConfUtilties;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class SentryService implements Callable {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SentryService.class);

  private static enum Status {
    NOT_STARTED(), STARTED();
  }

  private final Configuration conf;
  private final InetSocketAddress address;
  private final int maxThreads;
  private final int minThreads;
  private boolean kerberos;
  private final String principal;
  private final String[] principalParts;
  private final String keytab;
  private final ExecutorService serviceExecutor;
  private Future future;
  private TServer thriftServer;
  private Status status;

  public SentryService(Configuration conf) {
    this.conf = conf;
    int port = conf
        .getInt(ServerConfig.RPC_PORT, ServerConfig.RPC_PORT_DEFAULT);
    if (port == 0) {
      port = findFreePort();
    }
    this.address = NetUtils.createSocketAddr(
        conf.get(ServerConfig.RPC_ADDRESS, ServerConfig.RPC_ADDRESS_DEFAULT),
        port);
    LOGGER.info("Configured on address " + address);
    kerberos = ServerConfig.SECURITY_MODE_KERBEROS.equalsIgnoreCase(
        conf.get(ServerConfig.SECURITY_MODE, ServerConfig.SECURITY_MODE_KERBEROS).trim());
    maxThreads = conf.getInt(ServerConfig.RPC_MAX_THREADS,
        ServerConfig.RPC_MAX_THREADS_DEFAULT);
    minThreads = conf.getInt(ServerConfig.RPC_MIN_THREADS,
        ServerConfig.RPC_MIN_THREADS_DEFAULT);
    if (kerberos) {
      // Use Hadoop libraries to translate the _HOST placeholder with actual hostname
      try {
        String rawPrincipal = Preconditions.checkNotNull(conf.get(ServerConfig.PRINCIPAL), ServerConfig.PRINCIPAL + " is required");
        principal = SecurityUtil.getServerPrincipal(rawPrincipal, address.getAddress());
      } catch(IOException io) {
        throw new RuntimeException("Can't translate kerberos principal'", io);
      }
      LOGGER.info("Using kerberos principal: " + principal);

      principalParts = SaslRpcServer.splitKerberosName(principal);
      Preconditions.checkArgument(principalParts.length == 3,
          "Kerberos principal should have 3 parts: " + principal);
      keytab = Preconditions.checkNotNull(conf.get(ServerConfig.KEY_TAB),
          ServerConfig.KEY_TAB + " is required");
      File keytabFile = new File(keytab);
      Preconditions.checkState(keytabFile.isFile() && keytabFile.canRead(),
          "Keytab " + keytab + " does not exist or is not readable.");
    } else {
      principal = null;
      principalParts = null;
      keytab = null;
    }
    serviceExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
      private int count = 0;

      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, SentryService.class.getSimpleName() + "-"
            + (count++));
      }
    });
    status = Status.NOT_STARTED;
  }

  @Override
  public String call() throws Exception {
    SentryKerberosContext kerberosContext = null;
    try {
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
        ProcessorFactory factory = (ProcessorFactory) constructor
            .newInstance(conf);
        registeredProcessor = registeredProcessor
            || factory.register(processor);
      } catch (Exception e) {
        throw new IllegalStateException("Could not create "
            + processorFactory, e);
      }
    }
    if (!registeredProcessor) {
      throw new IllegalStateException(
          "Failed to register any processors from " + processorFactories);
    }
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
        .protocolFactory(new TBinaryProtocol.Factory())
        .minWorkerThreads(minThreads).maxWorkerThreads(maxThreads);
    thriftServer = new TThreadPoolServer(args);
    LOGGER.info("Serving on " + address);
    thriftServer.serve();
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
    status = Status.STARTED;
    future = serviceExecutor.submit(this);
  }

  public synchronized void stop() {
    if (status == Status.NOT_STARTED) {
      return;
    }
    LOGGER.info("Attempting to stop...");

    if (thriftServer.isServing()) {
      thriftServer.stop();
    }
    thriftServer = null;
    status = Status.NOT_STARTED;
    LOGGER.info("Stopped...");
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

  @SuppressWarnings("deprecation")
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
    conf.addResource(configFile.toURL());
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
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          LOGGER.info("ShutdownHook shutting down server");
          try {
            server.stop();
          } catch (Throwable t) {
            LOGGER.error("Error stopping SentryService", t);
          }
        }
      });

      // Let's wait on the service to stop
      try {
        LOGGER.info("Waiting on future.get()");
        server.future.get();
      } finally {
        server.serviceExecutor.shutdown();
      }
    }
  }

  public Configuration getConf() {
    return conf;
  }
}
