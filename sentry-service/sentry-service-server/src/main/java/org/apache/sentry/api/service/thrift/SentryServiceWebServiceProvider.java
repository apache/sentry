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

package org.apache.sentry.api.service.thrift;

import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;
import com.codahale.metrics.servlets.ThreadDumpServlet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EventListener;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.server.provider.webservice.WebServiceProvider;
import org.apache.sentry.server.provider.webservice.WebServiceProviderFactory;
import org.apache.sentry.server.provider.webservice.ServletDesc;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * WebServiceProvider instance to add base Service functions to the web interface
 */
public class SentryServiceWebServiceProvider implements WebServiceProvider,
    WebServiceProviderFactory {

  public static final String ID = "sentry-admin";

  private Configuration config;

  @Override
  public List<EventListener> getListeners() {
    return Arrays.asList(
        new SentryHealthCheckServletContextListener(),
        new SentryMetricsServletContextListener());
  }

  @Override
  public List<ServletDesc> getServlets() {
    List<ServletDesc> servlets = new ArrayList<>();
    servlets.add(ServletDesc.of("/metrics", new ServletHolder(MetricsServlet.class)));
    servlets.add(ServletDesc.of("/threads", new ServletHolder(ThreadDumpServlet.class)));
    servlets.add(ServletDesc.of("/healthcheck", new ServletHolder(HealthCheckServlet.class)));
    servlets.add(ServletDesc.of("/ping", new ServletHolder(PingServlet.class)));

    if (config.getBoolean(ServerConfig.SENTRY_WEB_ADMIN_SERVLET_ENABLED,
        ServerConfig.SENTRY_WEB_ADMIN_SERVLET_ENABLED_DEFAULT)) {
      servlets.add(ServletDesc.of("/admin/showAll", new ServletHolder(RolesServlet.class)));
      servlets.add(ServletDesc.of("/admin/roles", new ServletHolder(RolesServlet.class)));
    }
    return servlets;
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
