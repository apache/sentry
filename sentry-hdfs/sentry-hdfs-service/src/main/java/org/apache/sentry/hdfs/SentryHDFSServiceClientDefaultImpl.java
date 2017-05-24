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
package org.apache.sentry.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryHdfsServiceException;
import org.apache.sentry.core.common.transport.SentryConnection;
import org.apache.sentry.core.common.transport.SentryTransportPool;
import org.apache.sentry.core.common.transport.TTransportWrapper;
import org.apache.sentry.hdfs.ServiceConstants.ClientConfig;
import org.apache.sentry.hdfs.service.thrift.SentryHDFSService.Client;
import org.apache.sentry.hdfs.service.thrift.TAuthzUpdateResponse;
import org.apache.sentry.hdfs.service.thrift.TPathsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPermissionsUpdate;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Sentry HDFS Service Client
 * <p>
 * The class isn't thread-safe - it is up to the aller to ensure thread safety
 */
public class SentryHDFSServiceClientDefaultImpl
        implements SentryHDFSServiceClient, SentryConnection {
  private final boolean useCompactTransport;
  private Client client;
  private final SentryTransportPool transportPool;
  private TTransportWrapper transport;
  private final long maxMessageSize;

  SentryHDFSServiceClientDefaultImpl(Configuration conf,
                                     SentryTransportPool transportPool) throws IOException {
    maxMessageSize = conf.getLong(ClientConfig.SENTRY_HDFS_THRIFT_MAX_MESSAGE_SIZE,
            ClientConfig.SENTRY_HDFS_THRIFT_MAX_MESSAGE_SIZE_DEFAULT);
    useCompactTransport = conf.getBoolean(ClientConfig.USE_COMPACT_TRANSPORT,
            ClientConfig.USE_COMPACT_TRANSPORT_DEFAULT);
    this.transportPool = transportPool;
  }

  /**
   * Connect to the sentry server
   *
   * @throws Exception
   */
  @Override
  public void connect() throws Exception {
    if ((transport != null) && transport.isOpen()) {
      return;
    }

    transport = transportPool.getTransport();
    TProtocol tProtocol;
    if (useCompactTransport) {
      tProtocol = new TCompactProtocol(transport.getTTransport(), maxMessageSize, maxMessageSize);
    } else {
      tProtocol = new TBinaryProtocol(transport.getTTransport(), maxMessageSize, maxMessageSize, true, true);
    }
    TMultiplexedProtocol protocol = new TMultiplexedProtocol(
            tProtocol, SentryHDFSServiceClient.SENTRY_HDFS_SERVICE_NAME);

    client = new Client(protocol);
  }

  @Override
  public SentryAuthzUpdate getAllUpdatesFrom(long permSeqNum, long pathSeqNum)
          throws SentryHdfsServiceException {
    SentryAuthzUpdate retVal = new SentryAuthzUpdate(new LinkedList<PermissionsUpdate>(), new LinkedList<PathsUpdate>());
    try {
      TAuthzUpdateResponse sentryUpdates = client.get_all_authz_updates_from(permSeqNum, pathSeqNum);
      if (sentryUpdates.getAuthzPathUpdate() != null) {
        for (TPathsUpdate pathsUpdate : sentryUpdates.getAuthzPathUpdate()) {
          retVal.getPathUpdates().add(new PathsUpdate(pathsUpdate));
        }
      }
      if (sentryUpdates.getAuthzPermUpdate() != null) {
        for (TPermissionsUpdate permsUpdate : sentryUpdates.getAuthzPermUpdate()) {
          retVal.getPermUpdates().add(new PermissionsUpdate(permsUpdate));
        }
      }
    } catch (Exception e) {
      throw new SentryHdfsServiceException("Thrift Exception occurred !!", e);
    }
    return retVal;
  }

  @Override
  public void close() {
    done();
  }

  @Override
  public void done() {
    if (transport != null) {
      transportPool.returnTransport(transport);
      transport = null;
    }
  }

  @Override
  public void invalidate() {
    if (transport != null) {
      transportPool.invalidateTransport(transport);
      transport = null;
    }
  }
}
