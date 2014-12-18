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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;

import com.hazelcast.core.HazelcastInstance;

/**
 * An implementation of {@link LockingSentryStore} that implements the Locking
 * strategy using a Zookeeper based Lock. It uses the Curator Library for the
 * globally consistent inter-process read-write lock implementation that it
 * provides.
 */

public class SentryStoreWithDistributedLock extends
    LockingSentryStore<SentryStoreWithDistributedLock.DistributedLockContext> {

  public static final String SENTRY_DISTRIBUTED_LOCK_TIMEOUT_MS =
      "sentry.distributed.lock.timeout.ms";
  public static final int SENTRY_DISTRIBUTED_LOCK_TIMEOUT_MS_DEF = 10000;
  public static final String SENTRY_DISTRIBUTED_LOCK_PATH =
      "sentry.distributed.lock.path";
  public static final String SENTRY_DISTRIBUTED_LOCK_PATH_DEF =
      "/sentryStorePath";

  class DistributedLockContext implements
      LockingSentryStore.LockContext {
    final InterProcessMutex mutex;

    DistributedLockContext(InterProcessMutex mutex) {
      this.mutex = mutex;
    }
    @Override
    public void unlock() {
      try {
        mutex.release();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private final InterProcessReadWriteLock lock;
  private final int lockTimeoutMs;

  public SentryStoreWithDistributedLock(SentryStore sentryStore, HAContext haContext) {
    super(sentryStore);
    lockTimeoutMs =
        getConfiguration().getInt(SENTRY_DISTRIBUTED_LOCK_TIMEOUT_MS,
            SENTRY_DISTRIBUTED_LOCK_TIMEOUT_MS_DEF);
    lock = new InterProcessReadWriteLock(haContext.getCuratorFramework(), haContext.getNamespace()
        + getConfiguration().get(SENTRY_DISTRIBUTED_LOCK_PATH,
            SENTRY_DISTRIBUTED_LOCK_PATH_DEF));
  }

  private DistributedLockContext lock(InterProcessMutex mutex) {
    try {
      if (mutex.acquire(lockTimeoutMs, TimeUnit.MILLISECONDS)) {
        return new DistributedLockContext(mutex);
      }
    } catch (Exception ie) {
      throw new RuntimeException(ie);
    }
    throw new RuntimeException("Could not acquire lock !!");
  }

  @Override
  protected DistributedLockContext writeLock() {
    return lock(lock.writeLock());
  }

  @Override
  protected DistributedLockContext readLock() {
    return lock(lock.readLock());
  }
}
