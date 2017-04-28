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
package org.apache.sentry.hdfs;

import org.apache.sentry.core.common.exception.SentryHdfsServiceException;

public interface SentryHDFSServiceClient {
  public static final String SENTRY_HDFS_SERVICE_NAME = "SentryHDFSService";

  public void notifyHMSUpdate(PathsUpdate update)
      throws SentryHdfsServiceException;

  public long getLastSeenHMSPathSeqNum() throws SentryHdfsServiceException;

  public SentryAuthzUpdate getAllUpdatesFrom(long permSeqNum, long pathSeqNum)
      throws SentryHdfsServiceException;

  public void close();
}

