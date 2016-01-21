package org.apache.solr.handler.admin;

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

import org.apache.solr.core.CoreContainer;

/**
 * Secure version of InfoHandler that uses Sentry-aware request handlers
 */
public class SecureInfoHandler extends InfoHandler {

  public SecureInfoHandler(final CoreContainer coreContainer) {
    super(coreContainer);

    setPropertiesHandler( new SecureAdminHandlers.SecurePropertiesRequestHandler() );
    setThreadDumpHandler( new SecureAdminHandlers.SecureThreadDumpHandler() );
    setLoggingHandler( new SecureAdminHandlers.SecureLoggingHandler(coreContainer) );
    setSystemInfoHandler( new SecureAdminHandlers.SecureSystemInfoHandler(coreContainer) );
  }

}
