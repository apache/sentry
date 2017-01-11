/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.hdfs;


import junit.framework.Assert;
import org.apache.sentry.hdfs.service.thrift.TPermissionsUpdate;
import org.apache.sentry.hdfs.service.thrift.TPrivilegeChanges;
import org.apache.thrift.TException;
import org.junit.Test;

public class TestPermissionUpdate {

  @Test
  public void testSerializeDeserializeInJSON() throws TException {
    PermissionsUpdate update = new PermissionsUpdate(0, false);
    TPrivilegeChanges privUpdate = update.addPrivilegeUpdate(PermissionsUpdate.RENAME_PRIVS);
    privUpdate.putToAddPrivileges("newAuthz", "newAuthz");
    privUpdate.putToDelPrivileges("oldAuthz", "oldAuthz");

    // Serialize and deserialize the PermssionUpdate object should equals to the original one.
    TPermissionsUpdate before = update.toThrift();
    update.JSONDeserialize(update.JSONSerialize());
    Assert.assertEquals(before, update.toThrift());
  }
}
