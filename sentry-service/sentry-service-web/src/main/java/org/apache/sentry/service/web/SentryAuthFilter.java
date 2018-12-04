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

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Set;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.util.StringUtils;
import org.apache.sentry.service.common.ServiceConstants.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

/**
 * SentryAuthFilter is a subclass of AuthenticationFilter,
 * add authorization: Only allowed users could connect the web server.
 */
public class SentryAuthFilter extends AuthenticationFilter {

  private static final Logger LOG = LoggerFactory.getLogger(SentryAuthFilter.class);

  public static final String ALLOW_WEB_CONNECT_USERS = ServerConfig.SENTRY_WEB_SECURITY_ALLOW_CONNECT_USERS;

  private Set<String> allowUsers;

  @Override
  protected void doFilter(FilterChain filterChain, HttpServletRequest request,
      HttpServletResponse response) throws IOException, ServletException {
    String userName = request.getRemoteUser();
    LOG.debug("Authenticating user: " + userName + " from request.");
    if (!allowUsers.contains(userName)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN,
          "Unauthorized user status code: " + HttpServletResponse.SC_FORBIDDEN);
      throw new ServletException(userName + " is unauthorized. status code: " + HttpServletResponse.SC_FORBIDDEN);
    }
    super.doFilter(filterChain, request, response);
  }

  /**
   * Override <code>getConfiguration<code> to get <code>ALLOW_WEB_CONNECT_USERS<code>.
   */
  @Override
  protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig) throws ServletException {
    Properties props = new Properties();
    Enumeration<?> names = filterConfig.getInitParameterNames();
    while (names.hasMoreElements()) {
      String name = (String) names.nextElement();
      if (name.startsWith(configPrefix)) {
        String value = filterConfig.getInitParameter(name);
        if (ALLOW_WEB_CONNECT_USERS.equals(name)) {
          allowUsers = parseConnectUsersFromConf(value);
        } else {
          props.put(name.substring(configPrefix.length()), value);
        }
      }
    }
    return props;
  }

  private static Set<String> parseConnectUsersFromConf(String value) {
    //Removed the logic to convert the allowed users to lower case, as user names need to be case sensitive
    return Sets.newHashSet(StringUtils.getStrings(value));
  }
}
