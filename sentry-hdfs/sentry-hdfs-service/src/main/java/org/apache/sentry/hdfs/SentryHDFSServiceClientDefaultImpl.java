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
import org.apache.sentry.hdfs.service.thrift.TAuthzUpdateRequest;
import org.apache.sentry.hdfs.service.thrift.TAuthzUpdateResponse;
import org.apache.sentry.hdfs.service.thrift.TPathsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPermissionsUpdate;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.sentry.hdfs.service.thrift.sentry_hdfs_serviceConstants.UNUSED_PATH_UPDATE_IMG_NUM;

/**
 * Sentry HDFS Service Client
 * <p>
 * The class isn't thread-safe - it is up to the caller to ensure thread safety
 */
public class SentryHDFSServiceClientDefaultImpl
        implements SentryHDFSServiceClient, SentryConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryHDFSServiceClientDefaultImpl.class);

  private final boolean useCompactTransport;
  private Client client;
  private final SentryTransportPool transportPool;
  private TTransportWrapper transport;
  private final long maxMessageSize;

  SentryHDFSServiceClientDefaultImpl(Configuration conf,
                                     SentryTransportPool transportPool) {
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
      tProtocol = new TBinaryProtocol(transport.getTTransport(), maxMessageSize, maxMessageSize,
              true, true);
    }
    TMultiplexedProtocol protocol = new TMultiplexedProtocol(
            tProtocol, SentryHDFSServiceClient.SENTRY_HDFS_SERVICE_NAME);

    client = new Client(protocol);
  }

  @Override
  public SentryAuthzUpdate getAllUpdatesFrom(long permSeqNum, long pathSeqNum) throws SentryHdfsServiceException {
    return getAllUpdatesFrom(permSeqNum, pathSeqNum, UNUSED_PATH_UPDATE_IMG_NUM);
  }

  @Override
  public SentryAuthzUpdate getAllUpdatesFrom(long permSeqNum, long pathSeqNum, long pathImgNum)
          throws SentryHdfsServiceException {
    try {
      TAuthzUpdateRequest updateRequest = new TAuthzUpdateRequest(permSeqNum, pathSeqNum, pathImgNum);
      TAuthzUpdateResponse sentryUpdates = client.get_authz_updates(updateRequest);

      List<PathsUpdate> pathsUpdates = Collections.emptyList();
      if (sentryUpdates.getAuthzPathUpdate() != null) {
        pathsUpdates = new ArrayList<>(sentryUpdates.getAuthzPathUpdate().size());
        for (TPathsUpdate pathsUpdate : sentryUpdates.getAuthzPathUpdate()) {
          pathsUpdates.add(new PathsUpdate(pathsUpdate));
        }
      }

      List<PermissionsUpdate> permsUpdates = Collections.emptyList();
      if (sentryUpdates.getAuthzPermUpdate() != null) {
        permsUpdates = new ArrayList<>(sentryUpdates.getAuthzPermUpdate().size());
        for (TPermissionsUpdate permsUpdate : sentryUpdates.getAuthzPermUpdate()) {
          permsUpdates.add(new PermissionsUpdate(permsUpdate));
        }
      }

      if (LOGGER.isDebugEnabled() && !(permsUpdates.isEmpty() && pathsUpdates.isEmpty()) ) {
        LOGGER.debug("getAllUpdatesFrom({},{},{}): permsUpdates[{}], pathsUpdates[{}]",
          new Object[] { permSeqNum, pathSeqNum, pathImgNum, permsUpdates.size(), pathsUpdates.size() });
        if (LOGGER.isTraceEnabled()) {
          if (!permsUpdates.isEmpty()) {
            LOGGER.trace("permsUpdates{}", permsUpdates);
          }
          if (!pathsUpdates.isEmpty()) {
            LOGGER.trace("pathsUpdates{}", pathsUpdates);
          }
        }
      }

      return new SentryAuthzUpdate(permsUpdates, pathsUpdates);
    } catch (Exception e) {
      throw new SentryHdfsServiceException("Thrift Exception occurred !!", e);
    }
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
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("invalidate: " + transport);
      }
      transport = null;
    }
  }
}
