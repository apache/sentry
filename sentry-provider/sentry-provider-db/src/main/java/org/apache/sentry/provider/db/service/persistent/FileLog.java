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

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.provider.db.service.thrift.SentryConfigurationException;
import org.apache.sentry.provider.db.service.thrift.TSentryStoreRecord;
import org.apache.sentry.provider.db.service.thrift.TStoreSnapshot;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

/**
 * The FileLog class abstracts out the log file and all operations on it
 * When the FileLog is instantiated, It first checks if there is an existing
 * log file associated at the configured path. If yes, then the Client will
 * not be allowed to write to the log unless all existing LogEntries have been
 * iterated over using the standard iterator abstractions of {@link #hasNext()}
 * and {@link #next()}. Once all the entries are read out, it will open an
 * Output stream to the log file and clients can start logging.
 * The FileLog also support Log snapshots. If it see a special snapshot
 * record, the current log file is first closed and renamed. Once the snapshot
 * entry has been successfully written to a new log, it will delete the
 * old log file. 
 */
public class FileLog {

  // The default Java ObjectOutputStream cannot be appended to since it
  // writes a special header whenever you open the stream. We need this header
  // only when a new file is opened. When a store is shutdown and re-opened
  // again, subsequent appends should not write the header, else reading the
  // file again will throw a StreamCorruptedException.
  public class AppendingObjectOutputStream extends ObjectOutputStream {

    public AppendingObjectOutputStream(OutputStream out) throws IOException {
      super(out);
    }

    @Override
    protected void writeStreamHeader() throws IOException {
      // do not write a header
      reset();
    }
  }

  public static String SENTRY_FILE_LOG_STORE_LOCATION =
      "sentry.file.log.store.location";

  private static String COMMIT_LOG_FILE = "commit.log";

  public static class Entry {
    public final long seqId;
    public final TSentryStoreRecord record;
    public Entry(long seqId, TSentryStoreRecord record) {
      this.seqId = seqId;
      this.record = record;
    }
  }

  private volatile boolean isReady = false;
  private ObjectInputStream commitOis = null;
  private Entry nextEntry = null;
  private File logDir;

  private ObjectOutputStream commitLog;
  private final TSerializer serializer;
  private final TDeserializer deserializer;

  public FileLog(Configuration conf)
      throws SentryConfigurationException, FileNotFoundException, IOException {
    String currentDir = System.getProperty("user.dir");
    logDir = new File(conf.get(SENTRY_FILE_LOG_STORE_LOCATION, currentDir));
    TProtocolFactory protoFactory = new TCompactProtocol.Factory();
    serializer = new TSerializer(protoFactory);
    deserializer = new TDeserializer(protoFactory);
    if (logDir.exists()) {
      if (!logDir.isDirectory()) {
        throw new SentryConfigurationException(
            "Dir [" + logDir.getAbsolutePath() + "] exists and is not a directory !!");
      } else {
        if (new File(logDir, COMMIT_LOG_FILE).exists()) {
          commitOis =
              new ObjectInputStream(
                  new FileInputStream(new File(logDir, COMMIT_LOG_FILE)));
        } else {
          commitLog =
              new ObjectOutputStream(
                  new FileOutputStream(new File(logDir, COMMIT_LOG_FILE)));
          isReady = true;
        }
      }
    } else {
      isReady = true;
      boolean created = logDir.mkdirs();
      if (!created) {
        throw new RuntimeException(
            "Could not create store directory [" + logDir.getAbsolutePath() + "]");
      }
      commitLog =
          new ObjectOutputStream(new FileOutputStream(new File(logDir,
              COMMIT_LOG_FILE)));
    }
  }

  public boolean hasNext() {
    if (isReady) {
      return false;
    }
    if (nextEntry != null) {
      return true;
    }
    try {
      nextEntry = getNextEntry();
    } catch (EOFException e) {
      isReady = true;
      try {
        commitOis.close();
      } catch (Exception e2) {
        System.out.println("Got ex : " + e2.getMessage());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
    return !isReady;
  }

  private Entry getNextEntry() throws IOException, TException {
    long seqId = commitOis.readLong();
    int numRecordBytes = commitOis.readInt();
    byte[] recBytes = new byte[numRecordBytes];
    commitOis.readFully(recBytes);
    TSentryStoreRecord record = new TSentryStoreRecord();
    deserializer.deserialize(record, recBytes); 
    return new Entry(seqId, record);
  }

  // Must be called only after a hasNext();
  public Entry next() {
    Entry e = nextEntry;
    nextEntry = null;
    return e;
  }

  public void log(long seqId, TSentryStoreRecord record) {
    if (!isReady) {
      throw new RuntimeException("FileLog is not ready for writing yet !!");
    }
    try {
      prepareForSnapshotIfNeeded(seqId, record);
      if (commitLog == null) {
        commitLog =
            new AppendingObjectOutputStream(
                new FileOutputStream(new File(logDir, COMMIT_LOG_FILE), true));
      }
      byte[] recBytes = serializer.serialize(record);
      commitLog.writeLong(seqId);
      commitLog.writeInt(recBytes.length);
      commitLog.write(recBytes);
      commitLog.flush();
    } catch (Exception e) {
      throw new RuntimeException(
          "Could not log record with id [" + seqId + "] !!", e);
    }
    commitIfSnapshot(seqId, record);
  }

  // Truncate current log file and write the snapshot record
  private void prepareForSnapshotIfNeeded(long seqId, TSentryStoreRecord record)
      throws IOException {
    if (record.getSnapshot() != null) {
      // Close current log
      if (commitLog != null) {
        commitLog.flush();
        commitLog.close();
      }

      // Copy current log to temp
      boolean renameSuccess =
          new File(logDir, COMMIT_LOG_FILE)
              .renameTo(
              new File(logDir, COMMIT_LOG_FILE + "_tmp_" + seqId));
      if (!renameSuccess) {
        throw new IOException("Could not Prepare for snapshot !!");
      }
      commitLog = new ObjectOutputStream(
          new FileOutputStream(new File(logDir, COMMIT_LOG_FILE), true));;
    }
  }

  private void commitIfSnapshot(long seqId, TSentryStoreRecord record) {
    if (record.getSnapshot() != null) {
      new File(logDir, COMMIT_LOG_FILE + "_tmp_" + seqId).delete();
    }
  }

  public void close() {
    if (commitLog != null) {
      try {
        commitLog.flush();
        commitLog.close();
      } catch (IOException e) {
        System.out.println("Cound not close file : " + e.getMessage());
      }
    }
  }
}
