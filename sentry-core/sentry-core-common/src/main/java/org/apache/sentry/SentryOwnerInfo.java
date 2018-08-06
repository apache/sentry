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

package org.apache.sentry;

import org.apache.commons.lang.StringUtils;
import org.apache.sentry.service.common.ServiceConstants.SentryPrincipalType;

/**
 * This class holds the owner name and the Type
 */
public class SentryOwnerInfo {
  private String ownerName;
  private SentryPrincipalType ownerType;

  public SentryOwnerInfo (SentryPrincipalType type, String name) {
    ownerType = type;
    ownerName = name;
  }

  public void setOwnerName(String name) {
    ownerName = name;
  }

  public String getOwnerName() {
    return ownerName;
  }

  public SentryPrincipalType getOwnerType() {
    return ownerType;
  }

  public void setOwnerType(SentryPrincipalType type) {
    ownerType = type;
  }

  @Override
  public String toString() {
    return "Owner Type: " + ownerType.toString() + ", Owner Name: " + ownerName;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((ownerName == null) ? 0 : ownerName.hashCode());
    result = prime * result + ownerType.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    SentryOwnerInfo other = (SentryOwnerInfo) obj;

    if(ownerType != other.getOwnerType()) {
      return false;
    }
    return StringUtils.equals(ownerName, other.ownerName);
  }
}
