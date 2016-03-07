/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.sentry;

import java.io.File;
import java.lang.reflect.Constructor;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.codec.Charsets;

/**
 * Provides SentryIndexAuthorizationSingleton instance for use with
 * sentry-related unit tests.  In the unit tests, the primary
 * SentryIndexAuthorizationSingleton will be initialized without a sentry-site,
 * thus Sentry checking will not occur.  The SentryIndexAuthorizationSingleton
 * provided by getInstance in this class will be properly initialized for Sentry checking.
 *
 * NOTE: this is a hack, as there are multiple "singletons".  It may be cleaner
 * to just force the Sentry related tests to run in their own JVMs, so they
 * will always have the properly-initialized SentryIndexAuthorizationSingleton.
 */
public class SentrySingletonTestInstance {
  private static final Logger LOGGER = LoggerFactory.getLogger(SentrySingletonTestInstance.class);

  private static SentrySingletonTestInstance INSTANCE = new SentrySingletonTestInstance();
  private SentryIndexAuthorizationSingleton sentryInstance;
  private File sentrySite;

  private void addPropertyToSentry(StringBuilder builder, String name, String value) {
    builder.append("<property>\n");
    builder.append("<name>").append(name).append("</name>\n");
    builder.append("<value>").append(value).append("</value>\n");
    builder.append("</property>\n");
  }

  public void setupSentry() throws Exception {
    sentrySite = File.createTempFile("sentry-site", "xml");
    File authProviderDir = SolrTestCaseJ4.getFile("sentry-handlers/sentry");
    sentrySite.deleteOnExit();

    // need to write sentry-site at execution time because we don't know
    // the location of sentry.solr.provider.resource beforehand
    StringBuilder sentrySiteData = new StringBuilder();
    sentrySiteData.append("<configuration>\n");
    addPropertyToSentry(sentrySiteData, "sentry.provider",
      "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider");
    addPropertyToSentry(sentrySiteData, "sentry.solr.provider.resource",
       new File(authProviderDir.toString(), "test-authz-provider.ini").toURI().toURL().toString());
    sentrySiteData.append("</configuration>\n");
    FileUtils.writeStringToFile(sentrySite,sentrySiteData.toString(), Charsets.UTF_8.toString());
  }

  private SentrySingletonTestInstance() {
    try {
      setupSentry();
      Constructor ctor =
        SentryIndexAuthorizationSingleton.class.getDeclaredConstructor(String.class);
      ctor.setAccessible(true);
      sentryInstance =
        (SentryIndexAuthorizationSingleton)ctor.newInstance(sentrySite.toURI().toURL().toString().substring("file:".length()));
      // ensure all SecureAdminHandlers use this instance
      SecureRequestHandlerUtil.testOverride = sentryInstance;
    } catch (Exception ex) {
      LOGGER.error("Unable to create SentrySingletonTestInstance", ex);
    }
  }

  public static SentrySingletonTestInstance getInstance() {
    return INSTANCE;
  }

  public SentryIndexAuthorizationSingleton getSentryInstance() {
    return sentryInstance;
  }
}
