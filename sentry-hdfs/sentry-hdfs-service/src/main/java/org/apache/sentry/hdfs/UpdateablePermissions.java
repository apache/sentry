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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.sentry.hdfs.PermissionsUpdate;
import org.apache.sentry.hdfs.Updateable;
import org.apache.sentry.hdfs.UpdateForwarder.ExternalImageRetriever;

public class UpdateablePermissions implements Updateable<PermissionsUpdate>{
  private static final String UPDATABLE_TYPE_NAME = "perm_update";

  private AtomicLong seqNum = new AtomicLong();
  private final ExternalImageRetriever<PermissionsUpdate> imageRetreiver;

  public UpdateablePermissions(
      ExternalImageRetriever<PermissionsUpdate> imageRetreiver) {
    this.imageRetreiver = imageRetreiver;
  }

  @Override
  public PermissionsUpdate createFullImageUpdate(long currSeqNum) throws Exception {
    return imageRetreiver.retrieveFullImage(currSeqNum);
  }

  @Override
  public long getLastUpdatedSeqNum() {
    return seqNum.get();
  }

  @Override
  public void updatePartial(Iterable<PermissionsUpdate> update,
      ReadWriteLock lock) {
    for (PermissionsUpdate permsUpdate : update) {
      seqNum.set(permsUpdate.getSeqNum());
    }
  }

  @Override
  public Updateable<PermissionsUpdate> updateFull(PermissionsUpdate update) {
    UpdateablePermissions other = new UpdateablePermissions(imageRetreiver);
    other.seqNum.set(update.getSeqNum());
    return other;
  }

  @Override
  public String getUpdateableTypeName() {
    return UPDATABLE_TYPE_NAME;
  }

}
