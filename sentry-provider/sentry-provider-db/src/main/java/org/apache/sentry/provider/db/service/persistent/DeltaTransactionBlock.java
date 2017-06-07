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

import com.google.common.base.Preconditions;
import org.apache.sentry.hdfs.PathsUpdate;
import org.apache.sentry.hdfs.PermissionsUpdate;
import org.apache.sentry.provider.db.SentryInvalidInputException;
import org.apache.sentry.provider.db.service.model.MSentryHmsNotification;
import org.apache.sentry.provider.db.service.model.MSentryPathChange;
import org.apache.sentry.provider.db.service.model.MSentryPermChange;
import static org.apache.sentry.hdfs.Updateable.Update;

import javax.jdo.PersistenceManager;

/**
 * DeltaTransactionBlock is an implementation of {@link TransactionBlock}
 * that persists delta updates for {@link PathsUpdate} or {@link PermissionsUpdate}
 * into corresponding update table, e.g {@link MSentryPathChange} or
 * {@link MSentryPermChange}.
 * <p>
 * NullPointerException would be thrown if update is null.
 * {@link SentryInvalidInputException} would be thrown when update is
 * neither type of PathsUpdate nor PermissionsUpdate, also in the case
 * update contains a full image. TException would be thrown if Update
 * cannot be successfully serialized to JSON string.
 */
public class DeltaTransactionBlock implements TransactionBlock<Object> {
  private final Update update;

  public DeltaTransactionBlock(Update update) {
    this.update = update;
  }

  @Override
  public Object execute(PersistenceManager pm) throws Exception {
    persistUpdate(pm, update);
    return null;
  }

  /**
   * Persist the delta change into corresponding type based on its type.
   * Atomic increasing primary key changeID by 1.
   * <p>
   * NullPointerException would be thrown if update is null.
   * {@link SentryInvalidInputException} would be thrown when update is
   * neither type of PathsUpdate nor PermissionsUpdate. Also in the case
   * update contains a full image.
   * TException would be thrown if Update cannot be successfully serialized
   * to JSON string.
   *
   * @param pm PersistenceManager
   * @param update update
   * @throws Exception
   */
  private void persistUpdate(PersistenceManager pm, Update update)
      throws Exception {
    pm.setDetachAllOnCommit(false); // No need to detach objects

    Preconditions.checkNotNull(update);
    // persistUpdate cannot handle full image update, instead
    // it only handles delta updates.
    if (update.hasFullImage()) {
      throw new SentryInvalidInputException("Update should be not be a full image.\n");
    }

    // Persist the update into corresponding tables based on its type.
    // changeID is the primary key in MSentryPXXXChange table. If same
    // changeID is trying to be persisted twice, the transaction would
    // fail.
    if (update instanceof PermissionsUpdate) {
      long lastChangeID = SentryStore.getLastProcessedChangeIDCore(pm, MSentryPermChange.class);
      pm.makePersistent(new MSentryPermChange(lastChangeID + 1, (PermissionsUpdate) update));
    } else if (update instanceof PathsUpdate) {
      long lastChangeID = SentryStore.getLastProcessedChangeIDCore(pm, MSentryPathChange.class);
      pm.makePersistent(new MSentryPathChange(lastChangeID + 1, (PathsUpdate) update));
      // Notification id from PATH_UPDATE entry is made persistent in
      // SENTRY_LAST_NOTIFICATION_ID table.
      pm.makePersistent(new MSentryHmsNotification(update.getSeqNum()));
    } else {
      throw new SentryInvalidInputException("Update should be type of either " +
        "PermissionsUpdate or PathsUpdate.\n");
    }
  }
}
