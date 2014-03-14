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

import static junit.framework.Assert.assertEquals;

import org.apache.sentry.core.model.db.AccessConstants;
import org.apache.sentry.provider.db.service.model.MSentryPrivilege;
import org.junit.Test;

public class TestSentryStoreToAuthorizable {

  private MSentryPrivilege privilege;

  @Test
  public void testServer() {
    privilege = new MSentryPrivilege(null, null, "server1", null, null, null, null);
    assertEquals("server=server1",
        SentryStore.toAuthorizable(privilege));
    privilege = new MSentryPrivilege(null, null, "server1", null, null, null,
        AccessConstants.ALL);
    assertEquals("server=server1->action=*",
        SentryStore.toAuthorizable(privilege));
  }

  @Test
  public void testTable() {
    privilege = new MSentryPrivilege(null, null, "server1", "db1", "tbl1", null, null);
    assertEquals("server=server1->db=db1->table=tbl1",
        SentryStore.toAuthorizable(privilege));
    privilege = new MSentryPrivilege(null, null, "server1", "db1", "tbl1", null,
        AccessConstants.INSERT);
    assertEquals("server=server1->db=db1->table=tbl1->action=insert",
        SentryStore.toAuthorizable(privilege));
    privilege = new MSentryPrivilege(null, null, "server1", "db1", "tbl1", null,
        AccessConstants.SELECT);
    assertEquals("server=server1->db=db1->table=tbl1->action=select",
        SentryStore.toAuthorizable(privilege));
    privilege = new MSentryPrivilege(null, null, "server1", "db1", "tbl1", null,
        AccessConstants.ALL);
    assertEquals("server=server1->db=db1->table=tbl1->action=*",
        SentryStore.toAuthorizable(privilege));
  }

  @Test
  public void testDb() {
    privilege = new MSentryPrivilege(null, null, "server1", "db1", null, null, null);
    assertEquals("server=server1->db=db1",
        SentryStore.toAuthorizable(privilege));
    privilege = new MSentryPrivilege(null, null, "server1", "db1", null, null,
        AccessConstants.ALL);
    assertEquals("server=server1->db=db1->action=*",
        SentryStore.toAuthorizable(privilege));
  }

  @Test
  public void testUri() {
    privilege = new MSentryPrivilege(null, null, "server1", null, null, "file:///", null);
    assertEquals("server=server1->uri=file:///",
        SentryStore.toAuthorizable(privilege));
    privilege = new MSentryPrivilege(null, null, "server1", null, null, "file:///",
        AccessConstants.SELECT);
    assertEquals("server=server1->uri=file:///->action=select",
        SentryStore.toAuthorizable(privilege));
    privilege = new MSentryPrivilege(null, null, "server1", null, null, "file:///",
        AccessConstants.ALL);
    assertEquals("server=server1->uri=file:///->action=*",
        SentryStore.toAuthorizable(privilege));
  }
}
