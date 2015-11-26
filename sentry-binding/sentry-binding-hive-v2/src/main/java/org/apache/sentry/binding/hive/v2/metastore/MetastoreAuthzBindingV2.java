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
package org.apache.sentry.binding.hive.v2.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.PreDropPartitionEvent;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.sentry.binding.metastore.MetastoreAuthzBinding;

/**
 * Sentry binding for Hive Metastore. The binding is integrated into Metastore
 * via the pre-event listener which are fired prior to executing the metadata
 * action. This point we are only authorizing metadata writes since the listners
 * are not fired from read events. Each action builds a input and output
 * hierarchy as per the objects used in the given operations. This is then
 * passed down to the hive binding which handles the authorization. This ensures
 * that we follow the same privilege model and policies.
 */
public class MetastoreAuthzBindingV2 extends MetastoreAuthzBinding {

  public MetastoreAuthzBindingV2(Configuration config) throws Exception {
    super(config);
  }

  protected void authorizeDropPartition(PreDropPartitionEvent context)
      throws InvalidOperationException, MetaException {
    authorizeMetastoreAccess(
        HiveOperation.ALTERTABLE_DROPPARTS,
        new HierarcyBuilder().addTableToOutput(getAuthServer(),
            context.getTable().getDbName(),
            context.getTable().getTableName()).build(),
        new HierarcyBuilder().addTableToOutput(getAuthServer(),
            context.getTable().getDbName(),
            context.getTable().getTableName()).build());
  }

}
