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

package org.apache.sentry.service.thrift;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;

public final class SentryServiceUtil {
  /**
   * Gracefully shut down an Executor service.
   * <p>
   * This code is based on the Javadoc example for the Executor service.
   * <p>
   * First call shutdown to reject incoming tasks, and then call
   * shutdownNow, if necessary, to cancel any lingering tasks.
   *
   * @param pool the executor service to shut down
   * @param poolName the name of the executor service to shut down to make it easy for debugging
   * @param timeout the timeout interval to wait for its termination
   * @param unit the unit of the timeout
   * @param logger the logger to log the error message if it cannot terminate. It could be null
   */
  static void shutdownAndAwaitTermination(ExecutorService pool, String poolName,
                       long timeout, TimeUnit unit, Logger logger) {
    Preconditions.checkNotNull(pool);

    pool.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(timeout, unit)) {
        pool.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if ((!pool.awaitTermination(timeout, unit)) && (logger != null)) {
          logger.error("Executor service {} did not terminate",
              StringUtils.defaultIfBlank(poolName, "null"));
        }
      }
    } catch (InterruptedException ignored) {
      // (Re-)Cancel if current thread also interrupted
      pool.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Checks if Sentry is configured with HDFS sync enabled.
   *
   * @param conf The Configuration object where HDFS sync configurations are set.
   * @return True if enabled; False otherwise.
   */
  static boolean isHDFSSyncEnabled(Configuration conf) {
    List<String> processorFactories =
        Arrays.asList(conf.get(ServiceConstants.ServerConfig.PROCESSOR_FACTORIES, "").split(","));

    List<String> policyStorePlugins =
        Arrays.asList(conf.get(ServiceConstants.ServerConfig.SENTRY_POLICY_STORE_PLUGINS, "").split(","));


    return processorFactories.contains("org.apache.sentry.hdfs.SentryHDFSServiceProcessorFactory")
        && policyStorePlugins.contains("org.apache.sentry.hdfs.SentryPlugin");
  }

  static String getHiveMetastoreURI() {
    HiveConf hiveConf = new HiveConf();
    return hiveConf.get(METASTOREURIS.varname);
  }

  private SentryServiceUtil() {
    // Make constructor private to avoid instantiation
  }

}
