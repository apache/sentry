package org.apache.solr.handler.component;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;

public class MockUserAttributeSource implements UserAttributeSource {
  @Override
  public Multimap<String, String> getAttributesForUser(String userName) {
    Multimap<String, String> results = LinkedHashMultimap.create();
    switch (userName) {
      case "user1":
        results.put("attr1", "val1");
        results.put("attr1", "val2");
        results.put("attr2", "val1");
        results.put("attr3", "val1");
        break;
      case "user2":
        results.put("attr1", "val1");
        results.put("attr1", "val2");
        results.put("attr1", "val3");
        results.put("attr1", "val4");
        break;
    }
    return results;
  }

  @Override
  public Class<? extends UserAttributeSourceParams> getParamsClass() {
    return null;
  }

  @Override
  public void init(UserAttributeSourceParams params, Collection<String> attributes) {

  }

}
