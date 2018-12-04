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

import com.google.gson.Gson;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.service.web.ConfServlet;

/**
 * Servlet for the presentation of the list of roles in the Sentry system.
 */
public class RolesServlet extends HttpServlet {

  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    Configuration conf = (Configuration)getServletContext().getAttribute(
        ConfServlet.CONF_CONTEXT_ATTRIBUTE);
    assert conf != null;

    Writer out = response.getWriter();
    try {
      SentryStore sentrystore = new SentryStore(conf);
      Map<String, Set<TSentryPrivilege>> roleMap = new HashMap<>();
      Set<String> roleSet = sentrystore.getAllRoleNames();
      for (String roleName: roleSet) {
        roleMap.put(roleName, sentrystore.getAllTSentryPrivilegesByRoleName(roleName));
      }
      String json = new Gson().toJson(roleMap);
      response.setContentType("application/json");
      response.setCharacterEncoding("UTF-8");
      out.write(json);
    } catch (Exception e) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
    }
    out.close();
  }
}
