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

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

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
   */
  public SimplePollingPolicy(final SimplePolicy policy,
      final File resourceFile, int pollingFrequency) {
    this.policy = policy;
    lastUpdateTimeStamp = new AtomicLong(resourceFile.lastModified());
    executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable target) {
        Thread thread = new Thread(target, Joiner.on("-").join(getClass().getSimpleName(),
            instanceCounter.incrementAndGet()));
        thread.setDaemon(true);
        return thread;
      }
    });
    LOGGER.info("Creating instance with " + resourceFile + " and polling" +
        " frequency " + pollingFrequency);
    if(!resourceFile.isFile()) {
      throw new IllegalArgumentException("Resource must either be a Shiro resource" +
          " or a file: " + resourceFile);
    }
    assert(resourceFile != null);
    executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        if(resourceFile.lastModified() > lastUpdateTimeStamp.get()) {
          lastUpdateTimeStamp.set(resourceFile.lastModified());
          try {
            policy.parse();
          } catch (Throwable throwable) {
            LOGGER.error("Unxpected error", throwable);
          }
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
  public ImmutableSet<String> getPermissions(String group) {
    return policy.getPermissions(group);
  }
}