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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class UpdateForwarder<K extends Updateable.Update> implements
    Updateable<K> {

  public static interface ExternalImageRetriever<K> {

    public K retrieveFullImage(long currSeqNum);

  }

  private final AtomicLong lastSeenSeqNum = new AtomicLong(0);
  private final AtomicLong lastCommittedSeqNum = new AtomicLong(0);
  // Updates should be handled in order
  private final Executor updateHandler = Executors.newSingleThreadExecutor();

  // Update log is used when propagate updates to a downstream cache.
  // The preUpdate log stores all commits that were applied to this cache.
  // When the update log is filled to capacity (updateLogSize), all
  // entries are cleared and a compact image if the state of the cache is
  // appended to the log.
  // The first entry in an update log (consequently the first preUpdate a
  // downstream cache sees) will be a full image. All subsequent entries are
  // partial edits
  private final LinkedList<K> updateLog = new LinkedList<K>();
  // UpdateLog is disabled when updateLogSize = 0;
  private final int updateLogSize;

  private final ExternalImageRetriever<K> imageRetreiver;

  private volatile Updateable<K> updateable;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private static final long INIT_SEQ_NUM = -2;

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateForwarder.class);

  public UpdateForwarder(Updateable<K> updateable,
      ExternalImageRetriever<K> imageRetreiver, int updateLogSize) {
    this(updateable, imageRetreiver, updateLogSize, 5000);
  }
  public UpdateForwarder(Updateable<K> updateable,
      ExternalImageRetriever<K> imageRetreiver, int updateLogSize,
      int initUpdateRetryDelay) {
    this.updateLogSize = updateLogSize;
    this.imageRetreiver = imageRetreiver;
    if (imageRetreiver != null) {
      spawnInitialUpdater(updateable, initUpdateRetryDelay);
    } else {
      this.updateable = updateable;
    }
  }

  private void spawnInitialUpdater(final Updateable<K> updateable,
      final int initUpdateRetryDelay) {
    K firstFullImage = null;
    try {
      firstFullImage = imageRetreiver.retrieveFullImage(INIT_SEQ_NUM);
    } catch (Exception e) {
      LOGGER.warn("InitialUpdater encountered exception !! ", e);
      firstFullImage = null;
      Thread initUpdater = new Thread() {
        @Override
        public void run() {
          while (UpdateForwarder.this.updateable == null) {
            try {
              Thread.sleep(initUpdateRetryDelay);
            } catch (InterruptedException e) {
              LOGGER.warn("Thread interrupted !! ", e);
              break;
            }
            K fullImage = null;
            try {
              fullImage =
                  UpdateForwarder.this.imageRetreiver
                  .retrieveFullImage(INIT_SEQ_NUM);
              appendToUpdateLog(fullImage);
            } catch (Exception e) {
              LOGGER.warn("InitialUpdater encountered exception !! ", e);
            }
            if (fullImage != null) {
              UpdateForwarder.this.updateable = updateable.updateFull(fullImage);
            }
          }
        }
      };
      initUpdater.start();
    }
    if (firstFullImage != null) {
      appendToUpdateLog(firstFullImage);
      this.updateable = updateable.updateFull(firstFullImage);
    }
  }
  /**
   * Handle notifications from HMS plug-in or upstream Cache
   * @param update
   */
  public void handleUpdateNotification(final K update) {
    // Correct the seqNums on the first update
    if (lastCommittedSeqNum.get() == INIT_SEQ_NUM) {
      K firstUpdate = updateLog.peek();
      long firstSeqNum = update.getSeqNum() - 1; 
      if (firstUpdate != null) {
        firstUpdate.setSeqNum(firstSeqNum);
      }
      lastCommittedSeqNum.set(firstSeqNum);
      lastSeenSeqNum.set(firstSeqNum);
    }
    final boolean editNotMissed = 
        lastSeenSeqNum.incrementAndGet() == update.getSeqNum();
    if (!editNotMissed) {
      lastSeenSeqNum.set(update.getSeqNum());
    }
    Runnable task = new Runnable() {
      @Override
      public void run() {
        K toUpdate = update;
        if (update.hasFullImage()) {
          updateable = updateable.updateFull(update);
        } else {
          if (editNotMissed) {
            // apply partial preUpdate
            updateable.updatePartial(Lists.newArrayList(update), lock);
          } else {
            // Retrieve full update from External Source and
            if (imageRetreiver != null) {
              toUpdate = imageRetreiver
                  .retrieveFullImage(update.getSeqNum());
              updateable = updateable.updateFull(toUpdate);
            }
          }
        }
        appendToUpdateLog(toUpdate);
      }
    };
    updateHandler.execute(task);
  }

  private void appendToUpdateLog(K update) {
    synchronized (updateLog) {
      if (updateLogSize > 0) {
        if (update.hasFullImage() || (updateLog.size() == updateLogSize)) {
          // Essentially a log compaction
          updateLog.clear();
          updateLog.add(update.hasFullImage() ? update
              : createFullImageUpdate(update.getSeqNum()));
        } else {
          updateLog.add(update);
        }
      }
      lastCommittedSeqNum.set(update.getSeqNum());
    }
  }

  /**
   * Return all updates from requested seqNum (inclusive)
   * @param seqNum
   * @return
   */
  public List<K> getAllUpdatesFrom(long seqNum) {
    List<K> retVal = new LinkedList<K>();
    synchronized (updateLog) {
      long currSeqNum = lastCommittedSeqNum.get();
      if (updateLogSize == 0) {
        // no updatelog configured..
        return retVal;
      }
      K head = updateLog.peek();
      if (head == null) {
        return retVal;
      }
      if (seqNum > currSeqNum + 1) {
        // This process has probably restarted since downstream
        // recieved last update
        retVal.addAll(updateLog);
        return retVal;
      }
      if (head.getSeqNum() > seqNum) {
        // Caller has diverged greatly..
        if (head.hasFullImage()) {
          // head is a refresh(full) image
          // Send full image along with partial updates
          for (K u : updateLog) {
            retVal.add(u);
          }
        } else {
          // Create a full image
          // clear updateLog
          // add fullImage to head of Log
          // NOTE : This should ideally never happen
          K fullImage = createFullImageUpdate(currSeqNum);
          updateLog.clear();
          updateLog.add(fullImage);
          retVal.add(fullImage);
        }
      } else {
        // increment iterator to requested seqNum
        Iterator<K> iter = updateLog.iterator();
        K u = null;
        while (iter.hasNext()) {
          u = iter.next();
          if (u.getSeqNum() == seqNum) {
            break;
          }
        }
        // add all updates from requestedSeq
        // to committedSeqNum
        for (long seq = seqNum; seq <= currSeqNum; seq ++) {
          retVal.add(u);
          if (iter.hasNext()) {
            u = iter.next();
          } else {
            break;
          }
        }
      }
    }
    return retVal;
  }
 
  public boolean areAllUpdatesCommited() {
    return lastCommittedSeqNum.get() == lastSeenSeqNum.get();
  }

  public long getLastCommitted() {
    return lastCommittedSeqNum.get();
  }

  public long getLastSeen() {
    return lastSeenSeqNum.get();
  }

  @Override
  public Updateable<K> updateFull(K update) {
    return (updateable != null) ? updateable.updateFull(update) : null;
  }

  @Override
  public void updatePartial(Iterable<K> updates, ReadWriteLock lock) {
    if (updateable != null) {
      updateable.updatePartial(updates, lock);
    }
  }
  
  @Override
  public long getLastUpdatedSeqNum() {
    return (updateable != null) ? updateable.getLastUpdatedSeqNum() : INIT_SEQ_NUM;
  }

  @Override
  public K createFullImageUpdate(long currSeqNum) {
    return (updateable != null) ? updateable.createFullImageUpdate(currSeqNum) : null;
  }

}
