package org.apache.sentry.provider.db.service.thrift;

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

import java.io.IOException;
import java.io.Writer;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;

import static org.apache.commons.lang.StringEscapeUtils.escapeHtml;

/**
 * Servlet to print out all sentry configuration.
 */
public class ConfServlet extends HttpServlet {
  public static final String CONF_CONTEXT_ATTRIBUTE = "sentry.conf";
  public static final String FORMAT_JSON = "json";
  public static final String FORMAT_XML = "xml";
  public static final String FORMAT_PARAM = "format";
  private static final long serialVersionUID = 1L;

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    String format = request.getParameter(FORMAT_PARAM);
    if (format == null) {
      format = FORMAT_XML;
    }

    if (FORMAT_XML.equals(format)) {
      response.setContentType("text/xml; charset=utf-8");
    } else if (FORMAT_JSON.equals(format)) {
      response.setContentType("application/json; charset=utf-8");
    }

    Configuration conf = (Configuration)getServletContext().getAttribute(
        CONF_CONTEXT_ATTRIBUTE);
    assert conf != null;

    Writer out = response.getWriter();
    if (FORMAT_JSON.equals(format)) {
      Configuration.dumpConfiguration(conf, out);
    } else if (FORMAT_XML.equals(format)) {
      conf.writeXml(out);
    } else {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Bad format: " + escapeHtml(format));
    }
    out.close();
  }
}
