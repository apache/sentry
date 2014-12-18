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

package org.apache.sentry.provider.db.service.persistent;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.service.persistent.FileLog.Entry;
import org.apache.sentry.provider.db.service.thrift.TSentryStoreOp;
import org.apache.sentry.provider.db.service.thrift.TSentryStoreRecord;
import org.apache.thrift.TException;

/**
 * An implementation of the {@link PersistentSentryStore}. The Persistence
 * strategy used by this class is to log all write operations to a local file
 * using the {@link FileLog} log abstraction. Each write operation is stamp
 * with a monotonically +1 incrementing sequence Id. This guarantees that, after
 * a restart, the Sentry Store can read the log in the same order it was
 * written and would return to the same state it was before it was brought down.
 * The logging is also write-behind (operations are logged only after it has
 * been accepted by the underlying store) to ensure that erroneous operations
 * that would bring down the Store are not logged.
 * 
 * To limit the size of the log file, it requests the underlying SentryStore
 * to provide it with a snapshot of the store after a configurable number of
 * operations, which it writes to a new log file and and truncates the old one. 
 *
 */
public class SentryStoreWithFileLog extends
    PersistentSentryStore<SentryStoreWithFileLog.FileLogContext> {

  public static final int SENTRY_STORE_FILE_LOG_SNAPSHOT_THRESHOLD_DEF = 100;
  public static final String SENTRY_STORE_FILE_LOG_SNAPSHOT_THRESHOLD =
      "sentry.store.file.log.snapshot.threshold";

  /**
   * An implementation of the {@link PersistentContext} that is created prior
   * to the operation and stores the write record.
   */
  public static class FileLogContext implements
      PersistentSentryStore.PersistentContext {
    final long seqId;
    final TSentryStoreRecord record;

    FileLogContext(long seqId, TSentryStoreRecord record) {
      this.seqId = seqId;
      this.record = record;
    }
  }

  protected final FileLog fileLog;
  protected final AtomicLong lastSeenSeqId = new AtomicLong(0);
  protected final int snapshotThreshold;

  public SentryStoreWithFileLog(SentryStore sentryStore)
      throws FileNotFoundException, IOException, TException, SentryUserException {
    super(sentryStore);
    snapshotThreshold =
        getConfiguration().getInt(
            SENTRY_STORE_FILE_LOG_SNAPSHOT_THRESHOLD,
            SENTRY_STORE_FILE_LOG_SNAPSHOT_THRESHOLD_DEF);
    fileLog = new FileLog(getConfiguration());
    Entry ent = null;
    while (fileLog.hasNext()) {
      ent = fileLog.next();
      applyRecord(ent.record);
    }
    if (ent != null) {
      lastSeenSeqId.set(ent.seqId);
    }
  }

  @Override
  protected FileLogContext createRecord(TSentryStoreRecord record) {
    return new FileLogContext(lastSeenSeqId.incrementAndGet(), record);
  }

  @Override
  protected void onSuccess(FileLogContext context) {
    fileLog.log(context.seqId,
        getSnapshotIfRequired(context.seqId, context.record));
  }

  @Override
  protected void onFailure(FileLogContext context) {
    fileLog.log(context.seqId,
        getSnapshotIfRequired(context.seqId,
            new TSentryStoreRecord(TSentryStoreOp.NO_OP)));
  }

  protected TSentryStoreRecord getSnapshotIfRequired(long seqId, TSentryStoreRecord record) {
    if ((seqId > 0) && (seqId % snapshotThreshold == 0)) {
      if (record.getStoreOp() == TSentryStoreOp.SNAPSHOT) {
        return record;
      }
      TSentryStoreRecord snapshotRecord = new TSentryStoreRecord(TSentryStoreOp.SNAPSHOT);
      snapshotRecord.setSnapshot(getStore().toSnapshot());
      return snapshotRecord;
    } else {
      return record;
    }
  }

  @Override
  public void stop() {
    super.stop();
    fileLog.close();
  }

}
