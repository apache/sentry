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

package org.apache.sentry.service.thrift;

import junit.framework.TestCase;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Test for CounterWait class
 */
public class TestCounterWait extends TestCase {
  // Used to verify that wakeups happen in the right order
  private final BlockingDeque<Long> outSyncQueue = new LinkedBlockingDeque<>();

  public void testWaitFor() throws Exception {
    // Create a thread for each waiter
    int nthreads = 20;
    ExecutorService executor = Executors.newFixedThreadPool(nthreads);

    final CounterWait waiter = new CounterWait();

    // Initial value is zero, so this shouldn't block
    assertEquals(0, waiter.waitFor(0));

    // Create a pair of threads waiting for each value in [1, nthreads / 2]
    // We use pair of threads per value to verify that both are waken up
    for (int i = 0; i < nthreads; i++) {
      int finalI = i + 2;
      final int val = finalI / 2;
      executor.execute(new Runnable() {
                         public void run() {
                           long r = 0;
                           try {
                             r = waiter.waitFor(val); // blocks
                           } catch (InterruptedException e) {
                             e.printStackTrace();
                           }
                           outSyncQueue.add(r); // Once we wake up, post result
                         }
                       }
      );
    }

    // Wait until all threads are asleep.
    while(waiter.waitersCount() < nthreads) {
      sleep(20);
    }

    // All threads should be blocked, so outSyncQueue should be empty
    assertTrue(outSyncQueue.isEmpty());

    // Post a counter update for each value in [ 1, nthreads / 2 ]
    // After eac update two threads should be waken up and the corresponding pair of
    // values should appear in the outSyncQueue.
    for (int i = 0; i < (nthreads / 2); i++) {
      waiter.update(i + 1);
      long r = outSyncQueue.takeFirst();
      assertEquals(r, i + 1);
      r = outSyncQueue.takeFirst();
      assertEquals(r, i + 1);
      assertTrue(outSyncQueue.isEmpty());
    }

    // We are done
    executor.shutdown();
  }

  private void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
    }
  }
}
