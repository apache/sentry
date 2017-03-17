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
package org.apache.sentry.provider.db.service.thrift;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.persistent.SentryStore;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Admin Servlet is only used when SENTRY_WEB_ADMIN_SERVLET_ENABLED is true.
 */
public class SentryAdminServlet extends HttpServlet {
  private static final String SHOW_ALL = "/showAll";
  // Here we use the same way as in com.codahale.metrics.servlets.AdminServlet, and just
  // use the TEMPLATE as a static html with some links referenced to other debug pages.
  private static final String TEMPLATE = "<!DOCTYPE HTML>\n"+
      "<html lang=\"en\">\n"+
      "<head>\n"+
      "    <meta charset=\"utf-8\">\n"+
      "    <title>Sentry Service Admin</title>\n"+
      "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n"+
      "    <meta name=\"description\" content=\"\">\n"+
      "    <link href=\"css/bootstrap.min.css\" rel=\"stylesheet\">\n"+
      "    <link href=\"css/bootstrap-theme.min.css\" rel=\"stylesheet\">\n"+
      "    <link href=\"css/sentry.css\" rel=\"stylesheet\">\n"+
      "</head>\n"+
      "<body>\n"+
      "<nav class=\"navbar navbar-default navbar-fixed-top\">\n"+
      "    <div class=\"container\">\n"+
      "        <div class=\"navbar-header\">\n"+
      "            <a class=\"navbar-brand\" href=\"#\"><img src=\"sentry.png\" alt=\"Sentry Logo\"/></a>\n"+
      "        </div>\n"+
      "        <div class=\"collapse navbar-collapse\">\n"+
      "            <ul class=\"nav navbar-nav\">\n"+
      "                <li class=\"active\"><a href=\"#\">Admin</a></li>\n"+
      "                <li><a href=\"/metrics?pretty=true\">Metrics</a></li>\n"+
      "                <li><a href=\"/threads\">Threads</a></li>\n"+
      "                <li><a href=\"/conf\">Configuration</a></li>\n"+
      "                <li><a href=\"/admin/showAll\">ShowAllRoles</a></li>\n"+
      "            </ul>\n"+
      "        </div>\n"+
      "    </div>\n"+
      "</nav>\n"+
      "<div class=\"container\">\n"+
      "    <ul>\n"+
      "        <li><a href=\"/metrics?pretty=true\">Metrics</a></li>\n"+
      "        <li><a href=\"/threads\">Threads</a></li>\n"+
      "        <li><a href=\"/conf\">Configuration</a></li>\n"+
      "        <li><a href=\"/admin/showAll\">ShowAllRoles</a></li>\n"+
      "    </ul>\n"+
      "</div>\n"+
      "</body>\n"+
      "</html>";

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String uri = request.getPathInfo();
    if(uri != null && !uri.equals("/")) {
      if (uri.equals(SHOW_ALL)) {
        showAll(response);
      } else {
        response.sendError(404);
      }
    } else {
      response.setStatus(200);
      response.setHeader("Cache-Control", "must-revalidate,no-cache,no-store");
      response.setHeader("Pragma", "no-cache");
      response.setDateHeader("Expires", 0);
      response.setContentType("text/html");
      PrintWriter writer = response.getWriter();
      try {
        writer.println(TEMPLATE);
      } finally {
        writer.close();
      }
    }
  }

  /**
   * Print out all the roles and privileges information as json format.
   */
  private void showAll(HttpServletResponse response)
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
