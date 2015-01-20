package org.apache.sentry.provider.db.service.thrift;

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

import com.codahale.metrics.servlets.AdminServlet;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.EnumSet;
import java.util.EventListener;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.eclipse.jetty.server.DispatcherType;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SentryWebServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryWebServer.class);

  Server server;
  int port;

  public SentryWebServer(List<EventListener> listeners, int port, Configuration conf) {
    this.port = port;
    server = new Server(port);
    ServletContextHandler servletContextHandler = new ServletContextHandler();
    ServletHolder servletHolder =  new ServletHolder(AdminServlet.class);
    servletContextHandler.addServlet(servletHolder, "/*");

    for(EventListener listener:listeners) {
      servletContextHandler.addEventListener(listener);
    }

    String authMethod = conf.get(ServerConfig.SENTRY_WEB_SECURITY_TYPE);
    if (!ServerConfig.SENTRY_WEB_SECURITY_TYPE_NONE.equals(authMethod)) {
      /**
       * SentryAuthFilter is a subclass of AuthenticationFilter and
       * AuthenticationFilter tagged as private and unstable interface:
       * While there are not guarantees that this interface will not change,
       * it is fairly stable and used by other projects (ie - Oozie)
       */
      FilterHolder filterHolder = servletContextHandler.addFilter(SentryAuthFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST));
      filterHolder.setInitParameters(loadWebAuthenticationConf(conf));
    }
    server.setHandler(servletContextHandler);
  }

  public void start() throws Exception{
    server.start();
  }
  public void stop() throws Exception{
    server.stop();
  }
  public boolean isAlive() {
    return server != null && server.isStarted();
  }
  private static Map<String, String> loadWebAuthenticationConf(Configuration conf) {
    Map<String,String> prop = new HashMap<String, String>();
    prop.put(AuthenticationFilter.CONFIG_PREFIX, ServerConfig.SENTRY_WEB_SECURITY_PREFIX);
    String allowUsers = conf.get(ServerConfig.SENTRY_WEB_SECURITY_ALLOW_CONNECT_USERS);
    if (allowUsers == null || allowUsers.equals("")) {
      allowUsers = conf.get(ServerConfig.ALLOW_CONNECT);
      conf.set(ServerConfig.SENTRY_WEB_SECURITY_ALLOW_CONNECT_USERS, allowUsers);
    }
    validateConf(conf);
    for (Map.Entry<String, String> entry : conf) {
      String name = entry.getKey();
      if (name.startsWith(ServerConfig.SENTRY_WEB_SECURITY_PREFIX)) {
        String value = conf.get(name);
        prop.put(name, value);
      }
    }
    return prop;
  }

  private static void validateConf(Configuration conf) {
    String authHandlerName = conf.get(ServerConfig.SENTRY_WEB_SECURITY_TYPE);
    Preconditions.checkNotNull(authHandlerName, "Web authHandler should not be null.");
    String allowUsers = conf.get(ServerConfig.SENTRY_WEB_SECURITY_ALLOW_CONNECT_USERS);
    Preconditions.checkNotNull(allowUsers, "Allow connect user(s) should not be null.");
    if (ServerConfig.SENTRY_WEB_SECURITY_TYPE_KERBEROS.equalsIgnoreCase(authHandlerName)) {
      String principal = conf.get(ServerConfig.SENTRY_WEB_SECURITY_PRINCIPAL);
      Preconditions.checkNotNull(principal, "Kerberos principal should not be null.");
      Preconditions.checkArgument(principal.length() != 0, "Kerberos principal is not right.");
      String keytabFile = conf.get(ServerConfig.SENTRY_WEB_SECURITY_KEYTAB);
      Preconditions.checkNotNull(keytabFile, "Keytab File should not be null.");
      Preconditions.checkArgument(keytabFile.length() != 0, "Keytab File is not right.");
      try {
        UserGroupInformation.setConfiguration(conf);
        String hostPrincipal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
        UserGroupInformation.loginUserFromKeytab(hostPrincipal, keytabFile);
      } catch (IOException ex) {
        throw new IllegalArgumentException("Can't use Kerberos authentication, principal ["
          + principal + "] keytab [" + keytabFile + "]", ex);
      }
      LOGGER.info("Using Kerberos authentication, principal ["
          + principal + "] keytab [" + keytabFile + "]");
    }
  }
}