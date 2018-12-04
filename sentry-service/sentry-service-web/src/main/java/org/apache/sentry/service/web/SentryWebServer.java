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
 *
 */

package org.apache.sentry.service.web;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import java.util.EventListener;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.server.provider.webservice.AttributeDesc;
import org.apache.sentry.server.provider.webservice.FilterDesc;
import org.apache.sentry.server.provider.webservice.WebServiceProvider;
import org.apache.sentry.server.provider.webservice.WebServiceProviderFactory;
import org.apache.sentry.server.provider.webservice.WebServiceSpi;
import org.apache.sentry.server.provider.webservice.ServletDesc;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.apache.sentry.spi.ProviderManager;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;

@Slf4j
public class SentryWebServer {

  private Server server;

  public SentryWebServer(Configuration conf) {
    server = new Server();

    // Create a channel connector for "http/https" requests
    ServerConnector connector;
    int port = conf.getInt(ServerConfig.SENTRY_WEB_PORT, ServerConfig.SENTRY_WEB_PORT_DEFAULT);
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
    server.setConnectors(new Connector[]{connector});

    ServletContextHandler contextHandler = new ServletContextHandler();

    // Load all of the Web Service Provider

    // get the web service providers
    List<WebServiceProviderFactory> serviceProviderFactories = ProviderManager.getInstance()
        .load(WebServiceSpi.ID);

    // initialize the factories
    for (WebServiceProviderFactory providerFactory : serviceProviderFactories) {
      providerFactory.init(conf);
      WebServiceProvider provider = providerFactory.create();

      // register its listeners
      for (EventListener listener : provider.getListeners()) {
        contextHandler.addEventListener(listener);
      }

      //register its attributes
      for (AttributeDesc attributeEntry : provider.getAttributes()) {
        contextHandler.getServletContext()
            .setAttribute(attributeEntry.getName(), attributeEntry.getAttribute());
      }

      // register its servlets
      for (ServletDesc servletEntry : provider.getServlets()) {
        contextHandler
            .addServlet(servletEntry.getServletHolder(), servletEntry.getPathSpec());
      }

      // register its filters
      for (FilterDesc filterDesc : provider.getFilters()) {
        contextHandler.addFilter(filterDesc.getFilterHolder(), filterDesc.getPathSpec(),
            filterDesc.getDispatcherTypes());
      }
    }

    ContextHandlerCollection contextHandlerCollection = new ContextHandlerCollection();
    contextHandlerCollection.setHandlers(new Handler[]{contextHandler,
        new DefaultHandler()});

    server.setHandler(disableTraceMethod(contextHandlerCollection));
  }

  /**
   * Disables the HTTP TRACE method request which leads to Cross-Site Tracking (XST) problems.
   *
   * To disable it, we need to wrap the Handler (which has the HTTP TRACE enabled) with a constraint
   * that denies access to the HTTP TRACE method.
   *
   * @param handler The Handler which has the HTTP TRACE enabled.
   * @return A new Handler wrapped with the HTTP TRACE constraint and the Handler passed as
   * parameter.
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

  public void start() throws Exception {
    server.start();
  }

  public void stop() throws Exception {
    server.stop();
  }

  public boolean isAlive() {
    return server != null && server.isStarted();
  }


}
