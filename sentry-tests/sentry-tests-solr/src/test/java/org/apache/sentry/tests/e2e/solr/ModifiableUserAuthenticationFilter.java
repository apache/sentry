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
package org.apache.sentry.tests.e2e.solr;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.solr.servlet.SolrHadoopAuthenticationFilter;
import org.apache.solr.servlet.SolrRequestParsers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authentication Filter that authenticates any request as user "junit"
 */
public class ModifiableUserAuthenticationFilter implements Filter {
  private static final Logger LOG = LoggerFactory
    .getLogger(ModifiableUserAuthenticationFilter.class);

  /**
   * String that saves the user to be authenticated into Solr
   */
  private static String userName = "admin";

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    SolrRequestParsers.DEFAULT.setAddRequestHeadersToContext(true);
  }

  @Override
  public void destroy() {
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response,
                       FilterChain chain) throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    httpRequest.setAttribute(SolrHadoopAuthenticationFilter.USER_NAME, userName);
    chain.doFilter(request, response);
  }

  /**
   * Function to set the userName with the corresponding user passed as parameter
   * @param solrUser
   */
  public static void setUser(String solrUser) {
    userName = solrUser;
  }

  /**
   * Function to return the authenticated user name defined.
   * @param solrUser
   */
  public static String getUser() {
    return userName;
  }
}
