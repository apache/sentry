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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.DispatcherType;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.sentry.server.provider.webservice.AttributeDesc;
import org.apache.sentry.server.provider.webservice.FilterDesc;
import org.apache.sentry.server.provider.webservice.WebServiceProvider;
import org.apache.sentry.server.provider.webservice.WebServiceProviderFactory;
import org.apache.sentry.server.provider.webservice.ServletDesc;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * Provides the Web Interface functions for default Sentry services.
 */
@Slf4j
public class DefaultWebServicesProvider implements WebServiceProvider,
    WebServiceProviderFactory {

  public static final String ID = "default";
  private static final String STATIC_RESOURCE_DIR = "/webapp";

  private Configuration config;

  @Override
  public List<ServletDesc> getServlets() {
    List<ServletDesc> servlets = new ArrayList<>();
    servlets.add(ServletDesc.of("/conf", new ServletHolder(ConfServlet.class)));
    servlets.add(ServletDesc.of("/admin/logLevel", new ServletHolder(LogLevelServlet.class)));
    if (config.getBoolean(ServerConfig.SENTRY_WEB_PUBSUB_SERVLET_ENABLED,
        ServerConfig.SENTRY_WEB_PUBSUB_SERVLET_ENABLED_DEFAULT)) {
      servlets.add(ServletDesc.of("/admin/publishMessage", new ServletHolder(PubSubServlet.class)));
    }
    if (config.getBoolean(ServerConfig.SENTRY_WEB_ADMIN_SERVLET_ENABLED,
        ServerConfig.SENTRY_WEB_ADMIN_SERVLET_ENABLED_DEFAULT)) {
      // Static files holder
      ServletHolder staticHolder = new ServletHolder(new DefaultServlet());
      staticHolder.setInitParameter("pathInfoOnly", "true");
      URL url = this.getClass().getResource(STATIC_RESOURCE_DIR);
      staticHolder.setInitParameter("resourceBase", url.toString());
      servlets.add(ServletDesc.of("/*", staticHolder));
    }

    return servlets;
  }

  @Override
  public List<AttributeDesc> getAttributes() {
    return Arrays.asList(AttributeDesc.of(ConfServlet.CONF_CONTEXT_ATTRIBUTE, config));
  }

  @Override
  public List<FilterDesc> getFilters() {
    List<FilterDesc> filters = new ArrayList<>();
    String authMethod = config.get(ServerConfig.SENTRY_WEB_SECURITY_TYPE);
    if (!ServerConfig.SENTRY_WEB_SECURITY_TYPE_NONE.equalsIgnoreCase(authMethod)) {
      /**
       * SentryAuthFilter is a subclass of AuthenticationFilter and
       * AuthenticationFilter tagged as private and unstable interface:
       * While there are not guarantees that this interface will not change,
       * it is fairly stable and used by other projects (ie - Oozie)
       */
      FilterHolder sentryAuthFilterHolder = new FilterHolder(SentryAuthFilter.class);
      sentryAuthFilterHolder.setInitParameters(loadWebAuthenticationConf(config));
      filters.add(FilterDesc.of("/*", sentryAuthFilterHolder, EnumSet.of(DispatcherType.REQUEST)));
    }
    return filters;
  }

  private static Map<String, String> loadWebAuthenticationConf(Configuration conf) {
    Map<String, String> prop = new HashMap<String, String>();
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
        String hostPrincipal = SecurityUtil
            .getServerPrincipal(principal, ServerConfig.RPC_ADDRESS_DEFAULT);
        UserGroupInformation.loginUserFromKeytab(hostPrincipal, keytabFile);
      } catch (IOException ex) {
        throw new IllegalArgumentException("Can't use Kerberos authentication, principal ["
            + principal + "] keytab [" + keytabFile + "]", ex);
      }
      LOGGER
          .info("Using Kerberos authentication, principal [{}] keytab [{}]", principal, keytabFile);
    }
  }

  @Override
  public void init(Configuration config) {
    this.config = config;
  }

  @Override
  public WebServiceProvider create() {
    return this;
  }

  @Override
  public String getId() {
    return ID;
  }

  @Override
  public void close() {

  }
}
