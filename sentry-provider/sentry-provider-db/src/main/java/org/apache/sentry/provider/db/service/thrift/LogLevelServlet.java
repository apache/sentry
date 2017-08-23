/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.provider.db.service.thrift;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

import static org.apache.commons.lang.StringEscapeUtils.escapeHtml;

public class LogLevelServlet extends HttpServlet {
  private static final String LF = "\n";
  private static final String BR = "<br />";
  private static final String B_BR = "<b>%s</b><br />";
  private static final String FORMS_HEAD =
          "<h1>" + "Log Level" + "</h1>"
                  + LF + BR + "<hr /><h3>Results</h3>"
                  + LF + " Submitted Log Name: " + B_BR;
  private static final String FORMS_CONTENT_GET =
          LF + " Effective level: " + B_BR;
  private static final String FORMS_CONTENT_SET =
          LF + " Submitted Level: " + B_BR
                  + LF + " Setting Level to %s" + BR
                  + LF + " Effective level: " + B_BR;
  private static final String FORMS_END =
          LF + BR + "<hr /><h3>Get / Set</h3>"
                  + LF + "<form>Log: <input type='text' size='50' name='log' /> "
                  + "<input type='submit' value='Get Log Level' />" + "</form>"
                  + LF + "<form>Log: <input type='text' size='50' name='log' /> "
                  + "Level: <input type='text' name='level' /> "
                  + "<input type='submit' value='Set Log Level' />" + "</form>";
  private static final String FORMS_GET = FORMS_HEAD + FORMS_CONTENT_GET;
  private static final String FORMS_SET = FORMS_HEAD + FORMS_CONTENT_SET;

  /**
   * Return parameter on servlet request for the given name
   *
   * @param request: Servlet request
   * @param name: Name of parameter in servlet request
   * @return Parameter in servlet request for the given name, return null if can't find parameter.
   */
  private String getParameter(ServletRequest request, String name) {
    String s = request.getParameter(name);
    if (s == null) {
      return null;
    }
    s = s.trim();
    return s.length() == 0 ? null : s;
  }

  /**
   * Check the validity of the log level.
   * @param level: The log level to be checked
   * @return
   *        true: The log level is valid
   *        false: The log level is invalid
   */
  private boolean isLogLevelValid(String level) {
    return level.equals(Level.toLevel(level).toString());
  }

  /**
   * Parse the class name and log level in the http servlet request.
   * If the request contains only class name, return the log level in the response message.
   * If the request contains both class name and level, set the log level to the requested level
   * and return the setting result in the response message.
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
          throws ServletException, IOException {
    String logName = getParameter(request, "log");
    String level = getParameter(request, "level");
    response.setContentType("text/html;charset=utf-8");
    response.setStatus(HttpServletResponse.SC_OK);
    PrintWriter out = response.getWriter();

    if (logName != null) {
      Logger logInstance = LogManager.getLogger(logName);
      if (level == null) {
        out.write(String.format(FORMS_GET,
                escapeHtml(logName),
                logInstance.getEffectiveLevel().toString()));
      } else if (isLogLevelValid(level)) {
        logInstance.setLevel(Level.toLevel(level));
        out.write(String.format(FORMS_SET,
                escapeHtml(logName),
                level,
                level,
                logInstance.getEffectiveLevel().toString()));
      } else {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid log level: " + level);
        return;
      }
    }
    out.write(FORMS_END);
    out.close();
    response.flushBuffer();
  }
}
