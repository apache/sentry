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
import java.net.ServerSocket;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

public class SentryService implements Runnable {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SentryService.class);

  private static enum Status {
    NOT_STARTED(), STARTED();
  }

  private final Configuration conf;
  private final InetSocketAddress address;
  private final int maxThreads;
  private final int minThreads;
  private final String principal;
  private final String[] principalParts;
  private final String keytab;
  private final ExecutorService serviceExecutor;

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
    maxThreads = conf.getInt(ServerConfig.RPC_MAX_THREADS,
        ServerConfig.RPC_MAX_THREADS_DEFAULT);
    minThreads = conf.getInt(ServerConfig.RPC_MIN_THREADS,
        ServerConfig.RPC_MIN_THREADS_DEFAULT);
    principal = Preconditions.checkNotNull(conf.get(ServerConfig.PRINCIPAL),
        ServerConfig.PRINCIPAL + " is required");
    principalParts = SaslRpcServer.splitKerberosName(principal);
    Preconditions.checkArgument(principalParts.length == 3,
        "Kerberos principal should have 3 parts: " + principal);
    keytab = Preconditions.checkNotNull(conf.get(ServerConfig.KEY_TAB),
        ServerConfig.KEY_TAB + " is required");
    File keytabFile = new File(keytab);
    Preconditions.checkState(keytabFile.isFile() && keytabFile.canRead(),
        "Keytab " + keytab + " does not exist or is not readable.");
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
  public void run() {
    LoginContext loginContext = null;
    try {
      Subject subject = new Subject(false,
          Sets.newHashSet(new KerberosPrincipal(principal)),
          new HashSet<Object>(), new HashSet<Object>());
      loginContext = new LoginContext("", subject, null,
          KerberosConfiguration.createClientConfig(principal, new File(keytab)));
      loginContext.login();
      subject = loginContext.getSubject();
      Subject.doAs(subject, new PrivilegedExceptionAction<Void>() {
        @Override
        public Void run() throws Exception {
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
          TSaslServerTransport.Factory saslTransportFactory = new TSaslServerTransport.Factory();
          saslTransportFactory.addServerDefinition(AuthMethod.KERBEROS
              .getMechanismName(), principalParts[0], principalParts[1],
              ServerConfig.SASL_PROPERTIES, new GSSCallback(conf));
          TThreadPoolServer.Args args = new TThreadPoolServer.Args(
              serverTransport).processor(processor)
              .transportFactory(saslTransportFactory)
              .protocolFactory(new TBinaryProtocol.Factory())
              .minWorkerThreads(minThreads).maxWorkerThreads(maxThreads);
          thriftServer = new TThreadPoolServer(args);
          LOGGER.info("Serving on " + address);
          thriftServer.serve();
          return null;
        }
      });
    } catch (Throwable t) {
      LOGGER.error("Error starting server", t);
    } finally {
      status = Status.NOT_STARTED;
      if (loginContext != null) {
        try {
          loginContext.logout();
        } catch (LoginException e) {
          LOGGER.error("Error logging out", e);
        }
      }
    }
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  public synchronized boolean isRunning() {
    return status == Status.STARTED && thriftServer != null
        && thriftServer.isServing();
  }

  public synchronized void start() {
    if (status != Status.NOT_STARTED) {
      throw new IllegalStateException("Cannot start when " + status);
    }
    LOGGER.info("Attempting to start...");
    status = Status.STARTED;
    serviceExecutor.submit(this);
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
  public static class CommandImpl implements Command {
    @Override
    @SuppressWarnings("deprecation")
    public void run(String[] args) throws Exception {
      CommandLineParser parser = new GnuParser();
      Options options = new Options();
      options.addOption(null, ServiceConstants.ServiceArgs.CONFIG_FILE,
          true, "Sentry Service configuration file");
      CommandLine commandLine = parser.parse(options, args);
      String configFileName = commandLine.getOptionValue(ServiceConstants.
          ServiceArgs.CONFIG_FILE);
      File configFile = null;
      if (configFileName == null) {
        throw new IllegalArgumentException("Usage: " + ServiceConstants.ServiceArgs.CONFIG_FILE +
            " path/to/sentry-service.xml");
      } else if(!((configFile = new File(configFileName)).isFile() && configFile.canRead())) {
        throw new IllegalArgumentException("Cannot read configuration file " + configFile);
      }
      Configuration conf = new Configuration(false);
      conf.addResource(configFile.toURL());
      final SentryService server = new SentryService(conf);
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
    }
  }
}
