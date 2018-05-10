package org.apache.sentry.api.service.thrift;

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
import java.net.URL;
import java.util.EnumSet;
import java.util.EventListener;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import javax.servlet.DispatcherType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SentryWebServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryWebServer.class);
  private static final String RESOURCE_DIR = "/webapp";
  private static final String WELCOME_PAGE = "SentryService.html";

  private Server server;

  public SentryWebServer(List<EventListener> listeners, int port, Configuration conf) {
    server = new Server();

    // Create a channel connector for "http/https" requests
    ServerConnector connector;
    if (conf.getBoolean(ServerConfig.SENTRY_WEB_USE_SSL, false)) {
      SslContextFactory sslContextFactory = new SslContextFactory();
      sslContextFactory.setKeyStorePath(conf.get(ServerConfig.SENTRY_WEB_SSL_KEYSTORE_PATH, ""));
      sslContextFactory.setKeyStorePassword(
          conf.get(ServerConfig.SENTRY_WEB_SSL_KEYSTORE_PASSWORD, ""));
      // Exclude SSL blacklist protocols
      sslContextFactory.setExcludeProtocols(ServerConfig.SENTRY_SSL_PROTOCOL_BLACKLIST_DEFAULT);
      Set<String> moreExcludedSSLProtocols =
          Sets.newHashSet(Splitter.on(",").trimResults().omitEmptyStrings()
          .split(Strings.nullToEmpty(conf.get(ServerConfig.SENTRY_SSL_PROTOCOL_BLACKLIST))));
      sslContextFactory.addExcludeProtocols(moreExcludedSSLProtocols.toArray(
          new String[moreExcludedSSLProtocols.size()]));

      HttpConfiguration httpConfiguration = new HttpConfiguration();
      httpConfiguration.setSecurePort(port);
      httpConfiguration.setSecureScheme("https");
      httpConfiguration.addCustomizer(new SecureRequestCustomizer());

      connector = new ServerConnector(
          server,
          new SslConnectionFactory(sslContextFactory, "http/1.1"),
          new HttpConnectionFactory(httpConfiguration));

      LOGGER.info("Now using SSL mode.");
    } else {
      connector = new ServerConnector(server, new HttpConnectionFactory());
    }

    connector.setPort(port);
    server.setConnectors(new Connector[] { connector });

    ServletContextHandler servletContextHandler = new ServletContextHandler();
    ServletHolder servletHolder = new ServletHolder(AdminServlet.class);
    servletContextHandler.addServlet(servletHolder, "/*");

    for(EventListener listener:listeners) {
      servletContextHandler.addEventListener(listener);
    }

    servletContextHandler.addServlet(new ServletHolder(ConfServlet.class), "/conf");

    if (conf.getBoolean(ServerConfig.SENTRY_WEB_ADMIN_SERVLET_ENABLED,
        ServerConfig.SENTRY_WEB_ADMIN_SERVLET_ENABLED_DEFAULT)) {
      servletContextHandler.addServlet(
          new ServletHolder(SentryAdminServlet.class), "/admin/*");
    }
    servletContextHandler.getServletContext()
        .setAttribute(ConfServlet.CONF_CONTEXT_ATTRIBUTE, conf);

    servletContextHandler.addServlet(new ServletHolder(LogLevelServlet.class), "/admin/logLevel");

    if (conf.getBoolean(ServerConfig.SENTRY_WEB_PUBSUB_SERVLET_ENABLED,
                        ServerConfig.SENTRY_WEB_PUBSUB_SERVLET_ENABLED_DEFAULT)) {
      servletContextHandler.addServlet(new ServletHolder(PubSubServlet.class), "/admin/publishMessage");
    }

    ResourceHandler resourceHandler = new ResourceHandler();
    resourceHandler.setDirectoriesListed(true);
    URL url = this.getClass().getResource(RESOURCE_DIR);
    try {
      resourceHandler.setBaseResource(Resource.newResource(url.toString()));
    } catch (IOException e) {
      LOGGER.error("Got exception while setBaseResource for Sentry Service web UI", e);
    }
    resourceHandler.setWelcomeFiles(new String[]{WELCOME_PAGE});
    ContextHandler contextHandler= new ContextHandler();
    contextHandler.setHandler(resourceHandler);

    ContextHandlerCollection contextHandlerCollection = new ContextHandlerCollection();
    contextHandlerCollection.setHandlers(new Handler[]{contextHandler, servletContextHandler});

    String authMethod = conf.get(ServerConfig.SENTRY_WEB_SECURITY_TYPE);
    if (!ServerConfig.SENTRY_WEB_SECURITY_TYPE_NONE.equalsIgnoreCase(authMethod)) {
      /**
       * SentryAuthFilter is a subclass of AuthenticationFilter and
       * AuthenticationFilter tagged as private and unstable interface:
       * While there are not guarantees that this interface will not change,
       * it is fairly stable and used by other projects (ie - Oozie)
       */
      FilterHolder filterHolder = servletContextHandler.addFilter(SentryAuthFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST));
      filterHolder.setInitParameters(loadWebAuthenticationConf(conf));
    }

    server.setHandler(disableTraceMethod(contextHandlerCollection));
  }

  /**
   * Disables the HTTP TRACE method request which leads to Cross-Site Tracking (XST) problems.
   *
   * To disable it, we need to wrap the Handler (which has the HTTP TRACE enabled) with
   * a constraint that denies access to the HTTP TRACE method.
   *
   * @param handler The Handler which has the HTTP TRACE enabled.
   * @return A new Handler wrapped with the HTTP TRACE constraint and the Handler passed as parameter.
   */
  private Handler disableTraceMethod(Handler handler) {
    Constraint disableTraceConstraint = new Constraint();
    disableTraceConstraint.setName("Disable TRACE");
    disableTraceConstraint.setAuthenticate(true);

    ConstraintMapping mapping = new ConstraintMapping();
    mapping.setConstraint(disableTraceConstraint);
    mapping.setMethod("TRACE");
    mapping.setPathSpec("/");

    ConstraintSecurityHandler constraintSecurityHandler = new ConstraintSecurityHandler();
    constraintSecurityHandler.addConstraintMapping(mapping);
    constraintSecurityHandler.setHandler(handler);

    return constraintSecurityHandler;
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
        String hostPrincipal = SecurityUtil.getServerPrincipal(principal, ServerConfig.RPC_ADDRESS_DEFAULT);
        UserGroupInformation.loginUserFromKeytab(hostPrincipal, keytabFile);
      } catch (IOException ex) {
        throw new IllegalArgumentException("Can't use Kerberos authentication, principal ["
          + principal + "] keytab [" + keytabFile + "]", ex);
      }
      LOGGER.info("Using Kerberos authentication, principal [{}] keytab [{}]", principal, keytabFile);
    }
  }
}
