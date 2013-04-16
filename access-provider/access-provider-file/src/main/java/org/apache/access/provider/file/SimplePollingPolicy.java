/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.access.provider.file;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.access.core.Authorizable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSetMultimap;

public class SimplePollingPolicy implements Policy {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimplePollingPolicy.class);

  private static final AtomicInteger instanceCounter = new AtomicInteger(0);

  private final ScheduledExecutorService executor;
  private final AtomicLong lastUpdateTimeStamp;
  private final SimplePolicy policy;
  /**
   * @param resourcePath path to resource or file, {@link org.apache.shiro.io.ResourceUtils}
   * @param pollingFrequency interval to check for updates to the resource
   * @throws IOException
   */
  public SimplePollingPolicy(final SimplePolicy policy, int pollingFrequency) throws IOException {
    this.policy = policy;
    lastUpdateTimeStamp = new AtomicLong(policy.getModificationTime());
    executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable target) {
        Thread thread = new Thread(target, Joiner.on("-").join(getClass().getSimpleName(),
            instanceCounter.incrementAndGet()));
        thread.setDaemon(true);
        return thread;
      }
    });
    LOGGER.info("Creating instance polling frequency " + pollingFrequency);
    executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          long modificationTime = policy.getModificationTime();
          if(modificationTime > lastUpdateTimeStamp.get()) {
            lastUpdateTimeStamp.set(modificationTime);
              policy.parse();
          }
        } catch (Throwable throwable) {
          LOGGER.error("Unxpected error", throwable);
        }
      }
    }, pollingFrequency, pollingFrequency, TimeUnit.SECONDS);
  }

  public void shutdown() {
    executor.shutdown();
  }
  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableSetMultimap<String, String> getPermissions(List<Authorizable> authorizables, List<String> groups) {
    return policy.getPermissions(authorizables, groups);
  }
}