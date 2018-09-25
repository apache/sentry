/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.api.common;

import static junit.framework.Assert.assertFalse;
import static org.apache.sentry.service.common.ServiceConstants.ServerConfig.SENTRY_DB_EXPLICIT_GRANTS_PERMITTED;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Sets;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.api.service.thrift.TSentryPrivilege;
import org.apache.sentry.core.common.exception.SentryGrantDeniedException;
import org.junit.Test;

public class TestSentryServiceUtil {
  @Test
  public void testCheckPermittedDbGrants() {
    Configuration conf = new Configuration();

    // An empty configuration of permitted privileges should not throw exceptions
    try {
      SentryServiceUtil.checkDbExplicitGrantsPermitted(conf, Collections.emptySet());
    } catch (SentryGrantDeniedException e) {
      fail("An empty permitted privileges configuration should not throw an exception");
    }

    // Only ALL, SELECT and INSERT privileges are permitted
    conf.set(SENTRY_DB_EXPLICIT_GRANTS_PERMITTED, "all,*,select,insert");

    try {
      SentryServiceUtil.checkDbExplicitGrantsPermitted(conf, Sets.newHashSet(
        newTSentryPrivilege("select"), newTSentryPrivilege("insert"), newTSentryPrivilege("all"),
        newTSentryPrivilege("*")
      ));
    } catch (SentryGrantDeniedException e) {
      fail("ALL, *, SELECT and INSERT privileges should be permitted");
    }

    try {
      SentryServiceUtil.checkDbExplicitGrantsPermitted(conf, Sets.newHashSet(
        newTSentryPrivilege("select"), newTSentryPrivilege("create"), newTSentryPrivilege("all")
      ));
      fail("CREATE privileges should not be permitted");
    } catch (SentryGrantDeniedException e) {
      assertTrue("CREATE privileges should not be permitted", e.getMessage().contains("CREATE"));
      assertFalse("SELECT privileges should be permitted", e.getMessage().contains("SELECT"));
      assertFalse("ALL privileges should be permitted", e.getMessage().contains("ALL"));
    }

    conf.set(SENTRY_DB_EXPLICIT_GRANTS_PERMITTED, "select,insert,create");

    try {
      SentryServiceUtil.checkDbExplicitGrantsPermitted(conf, Sets.newHashSet(
        newTSentryPrivilege("alter"), newTSentryPrivilege("create"), newTSentryPrivilege("drop")
      ));
      fail("ALTER and DROP privileges should not be permitted");
    } catch (SentryGrantDeniedException e) {
      assertTrue("ALTER privileges should not be permitted", e.getMessage().contains("ALTER"));
      assertTrue("DROP privileges should not be permitted", e.getMessage().contains("DROP"));
      assertFalse("CREATE privileges should be permitted", e.getMessage().contains("CREATE"));
    }
  }

  private TSentryPrivilege newTSentryPrivilege(String action) {
    return new TSentryPrivilege("", "server1", action);
  }
}
