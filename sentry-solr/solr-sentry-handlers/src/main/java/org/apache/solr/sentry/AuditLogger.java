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


import org.apache.lucene.util.Version;
import org.noggit.CharArr;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Writes audit events to the audit log. This helps answer questions such as:
 * Who did what action when from where, and what values were changed from what
 * to what as a result?
 */
final class AuditLogger {

  public static final int ALLOWED = 1;
  public static final int UNAUTHORIZED = 0;

  private final Logger logger;

  private static final boolean IS_ENABLED =
    Boolean.valueOf(
      System.getProperty(AuditLogger.class.getName() + ".isEnabled", "true"));

  private static final String SOLR_VERSION = Version.LATEST.toString();


  public AuditLogger() {
    this.logger = LoggerFactory.getLogger(getClass());
  }

  public boolean isLogEnabled() {
    return IS_ENABLED && logger.isInfoEnabled();
  }

  public void log(
    String userName,
    String impersonator,
    String ipAddress,
    String operation,
    String operationParams,
    long eventTime,
    int allowed,
    String collectionName) {

    if (!isLogEnabled()) {
      return;
    }
    CharArr chars = new CharArr(512);
    JSONWriter writer = new JSONWriter(chars, -1);
    writer.startObject();
    writeField("solrVersion", SOLR_VERSION, writer);
    writer.writeValueSeparator();
    writeField("eventTime", eventTime, writer);
    writer.writeValueSeparator();
    writeField("allowed", allowed, writer);
    writer.writeValueSeparator();
    writeField("collectionName", collectionName, writer);
    writer.writeValueSeparator();
    writeField("operation", operation, writer);
    writer.writeValueSeparator();
    writeField("operationParams", operationParams, writer);
    writer.writeValueSeparator();
    writeField("ipAddress", ipAddress, writer);
    writer.writeValueSeparator();
    writeField("username", userName, writer);
    writer.writeValueSeparator();
    writeField("impersonator", impersonator, writer);
    writer.endObject();
    logger.info("{}", chars);
  }

  private void writeField(String key, Object value, JSONWriter writer) {
    writer.writeString(key);
    writer.writeNameSeparator();
    writer.write(value);
  }

}
