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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sentry.core.common.utils;

import java.util.List;

import com.google.common.collect.Lists;

public class KeyValue {
  private final String key;
  private final String value;

  public KeyValue(String keyValue) {
    int splitterIndex = keyValue.indexOf(SentryConstants.KV_SEPARATOR);
    int splitterIndexEnd = keyValue.lastIndexOf(SentryConstants.KV_SEPARATOR);

    if ((splitterIndex > 0) && (splitterIndex == splitterIndexEnd)) {
      // optimize for simple case
      key = keyValue.substring(0, splitterIndex).trim().intern();
      value = keyValue.substring(splitterIndex + 1).trim().intern();
    } else {
      List<String> kvList = Lists.newArrayList(SentryConstants.KV_SPLITTER.trimResults().limit(2).split(keyValue));
      if (kvList.size() != 2) {
        throw new IllegalArgumentException("Invalid key value: " + keyValue + " " + kvList);
      }
      key = kvList.get(0);
      value = kvList.get(1);
    }

    if (key.isEmpty()) {
      throw new IllegalArgumentException("For keyValue: " + keyValue + ", Key cannot be empty");
    } else if (value.isEmpty()) {
      throw new IllegalArgumentException("For keyValue: " + keyValue + ", Value cannot be empty");
    }
  }

  public KeyValue(String key, String value) {
    super();
    this.key = key.intern();
    this.value = value.intern();
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return SentryConstants.KV_JOINER.join(key, value);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((key == null) ? 0 : key.hashCode());
    result = prime * result + ((value == null) ? 0 : value.hashCode());
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
    KeyValue other = (KeyValue) obj;
    if (key == null) {
      if (other.key != null) {
        return false;
      }
    } else if (!key.equals(other.key)) {
      return false;
    }
    if (value == null) {
      if (other.value != null) {
        return false;
      }
    } else if (!value.equals(other.value)) {
      return false;
    }
    return true;
  }
}
