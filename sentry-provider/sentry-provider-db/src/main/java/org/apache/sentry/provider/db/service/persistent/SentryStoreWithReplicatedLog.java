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
import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.apache.sentry.SentryUserException;
import org.apache.sentry.provider.db.service.persistent.FileLog.Entry;
import org.apache.sentry.provider.db.service.thrift.TSentryStoreOp;
import org.apache.sentry.provider.db.service.thrift.TSentryStoreRecord;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.Member;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;

/**
 * A special subclass of the {@link SentryStoreWithFileLog} that uses the
 * same persistence strategy but in addition to logging to a local
 * {@link FileLog} will also publish the record to a distributed topic to be
 * consumed by other peer Stores in a Sentry cluster.
 * 
 * The class uses the Hazelcast library for :
 * 1) Distributed counter for sequence Id
 * 2) Distributed Topic with global ordering to replicate log entries to peer
 *    SentryStores
 *
 * Consistency Guarantees:
 * Hazelcast counters are globally consistent, but since it is optimized for
 * availability rather than consistency it can suffer from split-brain issues
 * in the event of a network partition. This should not be too much of an issue
 * for small deployments of 2-3 instances deployed in the same availability
 * zone, But strict consistency can be guaranteed by either:
 * 1) Wrapping this store with a {@link SentryStoreWithDistributedLock} which
 *    uses the Curator library's distributed read write lock which is based
 *    on Zookeeper. Zookeper has a stricter consistency model since all writes
 *    go thru a master.
 * 2) Deploying SentryService in active-standy mode which ensures writes are
 *    handled only by the active node. This can be trivially
 *    accomplished by setting the serivce discovery policy in the
 *    SentrtyPolicyServiceClient to 'STICKY' rather than the default
 *    'ROUND-ROBIN'
 *
 * Other Considerations:
 * This implementation also supports new peers joining an existing cluster.
 * The new peer will be brought upto speed with the other members of the
 * cluster in the following manner:
 * 1) All members listen to a special 'catchupRequest' topic.
 * 2) When a new member starts-up, it publishes a 'CatchupRequest' to the
 *    'catchUpRequest' topic. The CatchupRequest contains the last seen
 *    record's sequence number from its local FileLog.
 * 3) The new member also creates a uniquely named Distributed Queue and waits
 *    for messages on it. This queue name is also included in the
 *    CatchupRequest.
 * 4) This request is received by all the existing members of the cluster, it
 *    is ignored by all members EXCEPT the oldest member of the cluster which
 *    responds to the request by pushing the required records to the
 *    Distributed queue.
 */

public class SentryStoreWithReplicatedLog extends SentryStoreWithFileLog {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SentryStoreWithReplicatedLog.class);

  private static final int WAIT_SLEEP_MS = 500;

  /**
   * Only successful events are sent to the topic (failed records are sent as
   * no-op so that no gaps exist in the seqId)
   * Basically write behind logging.. If the publisher crashes before
   * writing to log, the update is not applied to the remote nodes.
   * Think this should be fine (It is similar to the case when Sentry
   * Service receives a message but dies while/before opening a db transaction)
   */
  public static class LogEntry implements Serializable {
    private static final long serialVersionUID = 8798360797372277777L;

    long seqId = -1;
    long nodeId = -1;
    byte[] recordBytes;

    public LogEntry() {}

    public LogEntry(long seqId, long nodeId, byte[] recordBytes) {
      this.seqId = seqId;
      this.nodeId = nodeId;
      this.recordBytes = recordBytes;
    }
  }

  public static class CatchUpRequest implements Serializable {
    private static final long serialVersionUID = -3345746198249394847L;

    long startSeqId = -1;
    long endSeqId = -1;
    long nodeId = -1;

    public CatchUpRequest() {}

    public CatchUpRequest(long startSeqId, long endSeqId, long nodeId) {
      this.startSeqId = startSeqId;
      this.endSeqId = endSeqId;
      this.nodeId = nodeId;
    }
  }

  class LogEntryWorker implements Runnable{
    @Override
    public void run() {
      while(true) {
        try {
          processLogEntry(entryQueue.take());
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (SentryUserException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private final TSerializer serializer;
  private final TDeserializer deserializer;
  private final BlockingQueue<LogEntry> entryQueue =
      new LinkedBlockingQueue<LogEntry>();
  private Thread entryWorker;

  // Distributed stuff
  private final HazelcastInstance hInst;
  private final long nodeId; 
  private final IAtomicLong globalSeqId;
  private final ITopic<LogEntry> dTopic;
  private final ITopic<CatchUpRequest> catchUpReqTopic;

  public SentryStoreWithReplicatedLog(SentryStore sentryStore)
      throws FileNotFoundException, IOException, TException,
      SentryUserException {
    this(sentryStore,
        DistributedUtils.getHazelcastInstance(sentryStore.getConfiguration(),
            true));
  }

  public SentryStoreWithReplicatedLog(SentryStore sentryStore,
      final HazelcastInstance hInst) throws FileNotFoundException, IOException,
      TException, SentryUserException {
    super(sentryStore);
    this.hInst = hInst;
    TProtocolFactory protoFactory = new TCompactProtocol.Factory();
    serializer = new TSerializer(protoFactory);
    deserializer = new TDeserializer(protoFactory);

    nodeId = hInst.getIdGenerator(DistributedUtils.SENTRY_STORE_NODEID).newId();
    globalSeqId = hInst.getAtomicLong(DistributedUtils.SENTRY_STORE_SEQID);
    dTopic = hInst.getTopic(DistributedUtils.SENTRY_DISTRIBUTED_TOPIC);
    dTopic.addMessageListener(new MessageListener<SentryStoreWithReplicatedLog.LogEntry>() {
      @Override
      public void onMessage(Message<LogEntry> msg) {
        LogEntry ent = msg.getMessageObject();
        try {
          entryQueue.put(ent);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });

    catchUpReqTopic = hInst.getTopic(DistributedUtils.SENTRY_CATCHUP_REQUEST_TOPIC);
    requestCatchUpIfRequired();
    catchUpReqTopic.addMessageListener(new MessageListener<CatchUpRequest>() {
      @Override
      public void onMessage(final Message<CatchUpRequest> req) {
        LOGGER.info("Catchup request received["
            + req.getMessageObject().startSeqId + ", "
            + req.getMessageObject().endSeqId + ", "
            + req.getMessageObject().nodeId + "]");
        handleCatchupRequest(req.getMessageObject(), hInst);
      }
    });

    entryWorker = new Thread(new LogEntryWorker(), "Log Entry Worker");
    entryWorker.start();
  }

  private void handleCatchupRequest(CatchUpRequest req, HazelcastInstance hInst) {
    Member oldestMember = hInst.getCluster().getMembers().iterator().next();
    // Am I oldest member ?
    if (oldestMember.localMember()) {
      long myLastSeen = lastSeenSeqId.get();
      if (req.endSeqId > myLastSeen) {
        LOGGER.info("Waiting for seq Id[" + myLastSeen + ", " + req.endSeqId + "]");
        try {
          Thread.sleep(getConfiguration().getInt(
              DistributedUtils.SENTRY_CATCHUP_WAIT_TIME,
              DistributedUtils.SENTRY_CATCHUP_WAIT_TIME_DEF));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      // Still not updated ?
      if (req.endSeqId > myLastSeen) {
        throw new RuntimeException(
            "Havnt recieved latest updated.. cannot respond to catchup request !!");
      }
      FileLog tempLog = null;
      try {
        tempLog = new FileLog(getStore().getConfiguration());
      } catch (Exception e) {
        throw new RuntimeException("Could not open FileLog to send catchup entries !!");
      }

      IQueue<LogEntry> respQueue =
          hInst.getQueue(DistributedUtils.SENTRY_CATCHUP_RESPONSE_QUEUE + req.nodeId);
      while (tempLog.hasNext()) {
        Entry entry = tempLog.next();
        if ((entry.seqId >= req.startSeqId)&&(entry.seqId <= req.endSeqId)) {
          try {
            respQueue.offer(new LogEntry(entry.seqId, nodeId, serializer.serialize(entry.record)));
            LOGGER.info("Sent Catchup entry [" + entry.seqId + ","
                + nodeId + ", " + req.nodeId + "]");
          } catch (TException e) {
            throw new RuntimeException("Could not send catchup entry !!", e);
          }
        }
      }
    }
  }

  private void requestCatchUpIfRequired() throws SentryUserException {
    long startSeqId = lastSeenSeqId.get() + 1;
    long endSeqId = globalSeqId.get();
    if (startSeqId <= endSeqId) {
      LOGGER.info("Sending Catchup request ["
          + startSeqId + ", "
          + endSeqId + ", "
          + nodeId + "]");
      // Send request for catchup entries
      IQueue<LogEntry> respQueue =
          hInst.getQueue(DistributedUtils.SENTRY_CATCHUP_RESPONSE_QUEUE + nodeId);
      catchUpReqTopic.publish(
          new CatchUpRequest(startSeqId, endSeqId, nodeId));
      receiveCatchUpEntries(respQueue, endSeqId);
    }
  }

  private void receiveCatchUpEntries(IQueue<LogEntry> catchUpQueue,
      long endSeqId) throws SentryUserException {
    long currSeqId = -1;
    while (currSeqId < endSeqId) {
      try {
        LogEntry entry =
            catchUpQueue.poll(
                getConfiguration().getInt(
                    DistributedUtils.SENTRY_CATCHUP_WAIT_TIME,
                    DistributedUtils.SENTRY_CATCHUP_WAIT_TIME_DEF),
                TimeUnit.MILLISECONDS);
        if (entry == null) {
          String msg =
              "Havnt received all catchup entries [" + currSeqId + ", "
                  + endSeqId + "]!!";
          LOGGER.error(msg);
          throw new RuntimeException(msg);
        }
        LOGGER.info("Received catchup [" + entry.seqId + ", " + entry.nodeId + "]");
        currSeqId = entry.seqId;
        processLogEntry(entry);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    catchUpQueue.destroy();
  }

  private void processLogEntry(LogEntry ent) throws SentryUserException {
    TSentryStoreRecord record = new TSentryStoreRecord();
    try {
      deserializer.deserialize(record, ent.recordBytes);
    } catch (TException e) {
      String msg = "Could not de-serialize record [" + ent.seqId + "]!!";
      Log.error(msg, e);
      throw new RuntimeException(msg, e);
    }
    // No need to update the publisher (Thats already done)
    if (ent.nodeId != SentryStoreWithReplicatedLog.this.nodeId) {
      try {
        applyRecord(record);
        fileLog.log(ent.seqId, getSnapshotIfRequired(ent.seqId, record));
        lastSeenSeqId.set(ent.seqId);
      } catch (SentryUserException e) {
        String msg = "Could not apply de-serialized record [" + ent.seqId + "]!!";
        Log.error(msg, e);
        throw new RuntimeException(msg, e);
      }
    }
  }

  @Override
  protected FileLogContext createRecord(TSentryStoreRecord record) {
    return new FileLogContext(globalSeqId.incrementAndGet(), record);
  }

  @Override
  protected void onSuccess(FileLogContext context) {
    super.onSuccess(context);
    lastSeenSeqId.set(context.seqId);
    try {
      dTopic.publish(new LogEntry(context.seqId, nodeId, serializer
          .serialize(context.record)));
    } catch (TException e) {
      throw new RuntimeException("Could not serialize Sentry record !!");
    }
  }

  @Override
  protected void onFailure(FileLogContext context) {
    // Publish a NO-OP record (since we dont want any gaps in the seqId)
    super.onFailure(context);
    lastSeenSeqId.set(context.seqId);
    try {
      dTopic.publish(new LogEntry(context.seqId, nodeId, serializer
          .serialize(new TSentryStoreRecord(TSentryStoreOp.NO_OP))));
    } catch (TException e) {
      throw new RuntimeException("Could not serialize Sentry record !!");
    }
  }

  public boolean waitForReplicattionToComplete(long timeInMs) {
    long totalWait = 0;
    while (true) {
      if (lastSeenSeqId.get() == globalSeqId.get()) {
        return true;
      }
      try {
        Thread.sleep(WAIT_SLEEP_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
      totalWait += WAIT_SLEEP_MS;
      if (totalWait >= timeInMs) {
        return false;
      }
    }
  }
}
