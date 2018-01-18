/*
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

import org.apache.http.annotation.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Waiting for counter to reach certain value.
 * The counter starts from zero and its value increases over time.
 * The class allows for multiple consumers waiting until the value of the
 * counter reaches some value interesting to them.
 * Consumers call {@link #waitFor(long)} which may either return
 * immediately if the counter reached the specified value, or block
 * until this value is reached. Consumers can also specify timeout for the
 * {@link #waitFor(long)} in which case it may return {@link TimeoutException}
 * when the wait was not successfull within the specified time limit.
 * <p>
 * All waiters should be waken up when the counter becomes equal or higher
 * then the value they are waiting for.
 * <p>
 * The counter is updated by a single updater that should only increase the
 * counter value.
 * The updater calls the {@link #update(long)} method to update the counter
 * value and this should wake up all threads waiting for any value smaller or
 * equal to the new one.
 * <p>
 * The class is thread-safe.
 * It is designed for use by multiple waiter threads and a single
 * updater thread, but it will work correctly even in the presence of multiple
 * updater threads.
 */
@ThreadSafe
public final class CounterWait {
  // Implementation notes.
  //
  // The implementation is based on:
  //
  // 1) Using an atomic counter value which guarantees consistency.
  //    Since everyone needs only to know when the counter value reached the
  //    certain value and the counter may only increase its value,
  //    it is safe to update the counter by another thread after its value
  //    was read.
  //
  // 2) Priority queue of waiters, sorted by their expected values. The smallest
  //    value is always at the top of the queue. The priority queue itself
  //    is thread-safe, so no locks are needed to protect access to it.
  //
  // Each waiter is implemented using a binary semaphore.
  // This solves the problem of a wakeup that happens before the sleep -
  // in this case the acquire() doesn't block and returns immediately.
  //
  // NOTE: We use PriorityBlockingQueue for waiters because it is thread-safe,
  //       we are not using its blocking queue semantics.

  private static final Logger LOGGER = LoggerFactory.getLogger(CounterWait.class);

  /** Counter value. May only increase. */
  private final AtomicLong currentId = new AtomicLong(0);

  private final long waitTimeout;
  private final TimeUnit waitTimeUnit;

  /**
   * Waiters sorted by the value of the counter they are waiting for.
   * Note that {@link PriorityBlockingQueue} is thread-safe.
   * We are not using this as a blocking queue, but as a synchronized
   * PriorityQueue.
   */
  private final PriorityBlockingQueue<ValueEvent> waiters =
          new PriorityBlockingQueue<>();

  /**
   * Create an instance of CounterWait object that will not timeout during wait
   */
  public CounterWait() {
    this(0, TimeUnit.SECONDS);
  }

  /**
   * Create an instance of CounterWait object that will timeout during wait
   * @param waitTimeout maximum time in seconds to wait for counter
   */
  public CounterWait(long waitTimeoutSec) {
    this(waitTimeoutSec, TimeUnit.SECONDS);
  }

  /**
   * Create an instance of CounterWait object that will timeout during wait
   * @param waitTimeout maximum time to wait for counter
   * @param waitTimeUnit time units for wait
   */
  public CounterWait(long waitTimeout, TimeUnit waitTimeUnit) {
    this.waitTimeout = waitTimeout;
    this.waitTimeUnit = waitTimeUnit;
  }

  /**
   * Update the counter value and wake up all threads waiting for this
   * value or any value below it.
   * <p>
   * The counter value should only increase.
   * An attempt to decrease the value is ignored.
   *
   * @param newValue the new counter value
   */
  public synchronized void update(long newValue) {
    // update() is synchronized so the value can't change.
    long oldValue = currentId.get();
    LOGGER.debug("CounterWait update: oldValue = {}, newValue = {}", oldValue, newValue);
    // Avoid doing extra work if not needed
    if (oldValue == newValue) {
      return; // no-op
    }

    // Make sure the counter is never decremented.
    if (newValue < oldValue) {
      LOGGER.error("new counter value {} is smaller then the previous one {}",
              newValue, oldValue);
      return; // no-op
    }

    currentId.set(newValue);

    // Wake up any threads waiting for a counter to reach this value.
    wakeup(newValue);
  }

  /**
   * Explicitly reset the counter value to a new value, but allow setting to a
   * smaller value.
   * This should be used when we have some external event that resets the counter
   * value space.
   * @param newValue New counter value. If this is greater or equal then the current
   *                value, this is equivalent to {@link #update(long)}. Otherwise
   *                 sets the counter to the new smaller value.
   */
  public synchronized void reset(long newValue) {
    long oldValue = currentId.get();
    LOGGER.debug("CounterWait reset: oldValue = {}, newValue = {}", oldValue, newValue);

    if (newValue > oldValue) {
      update(newValue);
    } else if (newValue < oldValue) {
      LOGGER.warn("resetting counter from {} to smaller value {}",
              oldValue, newValue);
      currentId.set(newValue);
      // No need to wakeup waiters since no one should wait on the smaller value
    }
  }


  /**
   * Wait for specified counter value.
   * Returns immediately if the value is reached or blocks until the value
   * is reached.
   * Multiple threads can call the method concurrently.
   *
   * @param value requested counter value
   * @return current counter value that should be no smaller then the requested
   * value
   * @throws InterruptedException if the wait was interrupted, TimeoutException if
   * wait was not successfull within the timeout value specified at the construction time.
   */
  public long waitFor(long value) throws InterruptedException, TimeoutException {
    // Fast path - counter value already reached, no need to block
    if (value <= currentId.get()) {
      return currentId.get();
    }

    // Enqueue the waiter for this value
    ValueEvent eid = new ValueEvent(value);
    waiters.put(eid);

    // It is possible that between the fast path check and the time the
    // value event is enqueued, the counter value already reached the requested
    // value. In this case we return immediately.
    if (value <= currentId.get()) {
      return currentId.get();
    }

    // At this point we may be sure that by the time the event was enqueued,
    // the counter was below the requested value. This means that update()
    // is guaranteed to wake us up when the counter reaches the requested value.
    // The wake up may actually happen before we start waiting, in this case
    // the event's blocking queue will be non-empty and the waitFor() below
    // will not block, so it is safe to wake up before the wait.
    // So sit tight and wait patiently.
    eid.waitFor();
    LOGGER.debug("CounterWait added new value to waitFor: value = {}, currentId = {}", value, currentId.get());
    return currentId.get();
  }

  /**
   * Wake up any threads waiting for a counter to reach specified value
   * Peek at the top of the queue. If the queue is empty or the top value
   * exceeds the current value, we are done. Otherwise wakeup the top thread,
   * remove the corresponding waiter and continue.
   * <p>
   * Note that the waiter may be removed under our nose by
   * {@link #waitFor(long)} method, but this is Ok - in this case
   * waiters.remove() will just return false.
   *
   * @param value current counter value
   */
  private void wakeup(long value) {
    while (true) {
      // Get the top of the waiters queue or null if it is empty
      ValueEvent e = waiters.poll();
      if (e == null) {
        // Queue is empty - return.
        return;
      }
      // No one to wake up, return event to the queue and exit
      if (e.getValue() > value) {
        waiters.add(e);
        return;
      }
      // Due for wake-up call
      LOGGER.debug("CounterWait wakeup: event = {} is less than value = {}", e.getValue(), value);
      e.wakeup();
    }
  }

  // Useful for debugging
  @Override
  public String toString() {
    return "CounterWait{" + "currentId=" + currentId +
            ", waiters=" + waiters + "}";
  }

  /**
   * Return number of waiters. This is mostly useful for metrics/debugging
   *
   * @return number of sleeping waiters
   */
  public int waitersCount() {
    return waiters.size();
  }

  /**
   * Representation of the waiting event.
   * The waiting event consists of the expected value and a binary semaphore.
   * <p>
   * Each thread waiting for the given value, creates a ValueEvent and tries
   * to acquire a semaphore. This blocks until the semaphore is released.
   * <p>
   * ValueEvents are stored in priority queue sorted by value, so they should be
   * comparable by the value.
   */
  private class ValueEvent implements Comparable<ValueEvent> {
    /** Value waited for. */
    private final long value;
    /** Binary semaphore to synchronize waiters */
    private final Semaphore semaphore = new Semaphore(1);

    /**
     * Instantiates a new Value event.
     *
     * @param v the expected value
     */
    ValueEvent(long v) {
      this.value = v;
      // Acquire the semaphore. Subsequent calls to waitFor() will block until
      // wakeup() releases the semaphore.
      semaphore.acquireUninterruptibly(); // Will not block
    }

    /** Wait until signaled or interrupted. May return immediately if already signalled. */
    void waitFor() throws InterruptedException, TimeoutException {
      if (waitTimeout == 0) {
        semaphore.acquire();
        return;
      }
      if (!semaphore.tryAcquire(waitTimeout, waitTimeUnit)) {
        throw new TimeoutException();
      }
    }

    /** @return the value we are waiting for. */
    long getValue() {
      return value;
    }

    /** Wakeup the waiting thread. */
    void wakeup() {
      semaphore.release();
    }

    /**
     * Compare objects by value.
     */
    @Override
    public int compareTo(final ValueEvent o) {
      return value == o.value ? 0
              : value < o.value ? -1
              : 1;
    }

    /**
     * Use identity comparison of objects.
     */
    @Override
    public boolean equals(final Object o) {
      return (this == o);
    }

    @Override
    public int hashCode() {
      return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
      return String.valueOf(value);
    }
  }
}
