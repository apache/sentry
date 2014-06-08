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
package org.apache.sentry.provider.file;
import static org.apache.sentry.provider.file.PolicyFileConstants.KV_JOINER;
import static org.apache.sentry.provider.file.PolicyFileConstants.KV_SPLITTER;

import java.util.List;

import com.google.common.collect.Lists;

public class KeyValue {
  private final String key;
  private final String value;

  public KeyValue(String keyValue) {
    List<String> kvList = Lists.newArrayList(KV_SPLITTER.trimResults().limit(2).split(keyValue));
    if(kvList.size() != 2) {
      throw new IllegalArgumentException("Invalid key value: " + keyValue + " " + kvList);
    }
    key = kvList.get(0);
    value = kvList.get(1);
    if(key.isEmpty()) {
      throw new IllegalArgumentException("Key cannot be empty");
    } else if(value.isEmpty()) {
      throw new IllegalArgumentException("Value cannot be empty");
    }
  }
  public KeyValue(String key, String value) {
    super();
    this.key = key;
    this.value = value;
  }
  public String getKey() {
    return key;
  }
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return KV_JOINER.join(key, value);
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
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    KeyValue other = (KeyValue) obj;
    if (key == null) {
      if (other.key != null)
        return false;
    } else if (!key.equalsIgnoreCase(other.key))
      return false;
    if (value == null) {
      if (other.value != null)
        return false;
    } else if (!value.equalsIgnoreCase(other.value))
      return false;
    return true;
  }
}
