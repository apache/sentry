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
import java.security.Principal;
import java.util.Map;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.http.auth.BasicUserPrincipal;
import org.apache.solr.security.AuthenticationPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DummyAuthPluginImpl extends AuthenticationPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(DummyAuthPluginImpl.class);

  private static String userName = TestSentryServer.ADMIN_USER;

  @Override
  public void init(Map<String, Object> arg0) {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public boolean doAuthenticate(ServletRequest arg0, ServletResponse arg1, FilterChain arg2)
      throws Exception {
    HttpServletRequestWrapper wrapper = new HttpServletRequestWrapper((HttpServletRequest)arg0) {
      @Override
      public Principal getUserPrincipal() {
        return new BasicUserPrincipal(getUserName());
      }
    };
    arg2.doFilter(wrapper, arg1);
    return true;
  }

  public static synchronized void setUserName(String userName) {
    LOG.debug("Setting userName to {}", userName);
    DummyAuthPluginImpl.userName = userName;
  }

  public static synchronized String getUserName() {
    return userName;
  }
}
