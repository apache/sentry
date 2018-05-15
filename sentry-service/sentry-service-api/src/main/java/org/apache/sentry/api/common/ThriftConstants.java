/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sentry.api.common;

import org.apache.sentry.service.thrift.sentry_common_serviceConstants;

/**
 * Thrift generates terrible constant class names
 */
public class ThriftConstants extends sentry_common_serviceConstants {
  public static final int TSENTRY_SERVICE_VERSION_CURRENT = sentry_common_serviceConstants.TSENTRY_SERVICE_V2;
}

