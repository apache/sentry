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

import java.util.List;
import java.util.Map;

/**
 * A container for complete sentry permission snapshot.
 * <p>
 * It is composed by a role to groups mapping, and hiveObj to &lt role, privileges &gt mapping.
 * It also has the sequence number/change ID of latest delta change that the snapshot maps to.
 */
public class PermissionsImage {

  // A full snapshot of sentry role to groups mapping.
  private final Map<String, List<String>> roleImage;

  // A full snapshot of hiveObj to <role, privileges> mapping.
  private final Map<String, Map<String, String>> privilegeImage;
  private final long curSeqNum;

  public PermissionsImage(Map<String, List<String>> roleImage,
                          Map<String, Map<String, String>> privilegeImage, long curSeqNum) {
    this.roleImage = roleImage;
    this.privilegeImage = privilegeImage;
    this.curSeqNum = curSeqNum;
  }

  public long getCurSeqNum() {
    return curSeqNum;
  }

  public Map<String, Map<String, String>> getPrivilegeImage() {
    return privilegeImage;
  }

  public Map<String, List<String>> getRoleImage() {
    return roleImage;
  }
}
