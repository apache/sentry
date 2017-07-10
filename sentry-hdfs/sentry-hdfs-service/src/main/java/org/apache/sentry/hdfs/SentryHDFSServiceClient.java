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

/**
 * Private interface between HDFS NameNode and the Sentry Server.
 * The only exposed service is update exchange via {@link #getAllUpdatesFrom(long, long)}
 */
public interface SentryHDFSServiceClient extends AutoCloseable {
  /** Service name for Thrift */
  String SENTRY_HDFS_SERVICE_NAME = "SentryHDFSService";

  /**
   * Get any permission and path updates accumulated since given sequence numbers.
   * May return full update.
   * @param permSeqNum Last sequence number for permissions update processed by the NameNode plugin
   * @param pathSeqNum Last sequence number for paths update processed by the NameNode plugin
   * @return List of permission and path changes which may include a full snapshot.
   * @throws SentryHdfsServiceException if a connection exception happens
   */
  @Deprecated
  SentryAuthzUpdate getAllUpdatesFrom(long permSeqNum, long pathSeqNum)
          throws SentryHdfsServiceException;

  /**
   * Get any permission and path updates accumulated since given sequence numbers.
   * May return full update.
   * @param permSeqNum Last sequence number for permissions update processed by the NameNode plugin
   * @param pathSeqNum Last sequence number for paths update processed by the NameNode plugin
   * @param pathImgNum Last image number for paths update processed by the NameNode plugin
   * @return List of permission and path changes which may include a full snapshot.
   * @throws SentryHdfsServiceException if a connection exception happens
   */
  SentryAuthzUpdate getAllUpdatesFrom(long permSeqNum, long pathSeqNum, long pathImgNum)
      throws SentryHdfsServiceException;
}

