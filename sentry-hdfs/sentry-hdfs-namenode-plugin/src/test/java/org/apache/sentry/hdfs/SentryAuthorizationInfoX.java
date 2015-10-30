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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;

public class SentryAuthorizationInfoX extends SentryAuthorizationInfo {

  public SentryAuthorizationInfoX() {
    super(new String[]{"/user/authz"});
    System.setProperty("test.stale", "false");
  }

  @Override
  public void run() {
    
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public boolean isStale() {
    String stale = System.getProperty("test.stale");
    return stale.equalsIgnoreCase("true");
  }

  private static final String[] MANAGED = {"user", "authz"};
  private static final String[] AUTHZ_OBJ = {"user", "authz", "obj"};

  private boolean hasPrefix(String[] prefix, String[] pathElement) {
    int i = 0;
    for (; i < prefix.length && i < pathElement.length; i ++) {
      if (!prefix[i].equals(pathElement[i])) {
        return false;
      }
    }    
    return (i == prefix.length);
  }
  
  @Override
  public boolean isManaged(String[] pathElements) {
    return hasPrefix(MANAGED, pathElements);
  }

  @Override
  public boolean doesBelongToAuthzObject(String[] pathElements) {
    return hasPrefix(AUTHZ_OBJ, pathElements);
  }

  @Override
  public List<AclEntry> getAclEntries(String[] pathElements) {
    AclEntry acl = new AclEntry.Builder().setType(AclEntryType.USER).
        setPermission(FsAction.ALL).setName("user-authz").
        setScope(AclEntryScope.ACCESS).build();
    return Arrays.asList(acl);
  }
}
