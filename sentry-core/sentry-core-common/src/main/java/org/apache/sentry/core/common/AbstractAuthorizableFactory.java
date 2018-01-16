/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.core.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.sentry.core.common.utils.SentryConstants;

import java.util.List;

/**
 * @param <A> The type of Authorizable the class handles
 */
public abstract class AbstractAuthorizableFactory<A extends Authorizable, T extends AuthorizableType<A>> implements AuthorizableFactory<A, T> {

  @Override
  public A create(String s) {
    List<String> kvList = Lists.newArrayList(SentryConstants.KV_SPLITTER.trimResults().limit(2).split(s));
    if (kvList.size() != 2) {
      throw new IllegalArgumentException("Invalid authorizable string value: " + s + " " + kvList);
    }

    String type;
    String name;

    type = kvList.get(0);
    Preconditions.checkArgument(!type.isEmpty(), "Type cannot be empty");
    name = kvList.get(1);
    Preconditions.checkArgument(!name.isEmpty(), "Name cannot be empty");
    try {
      return create(type, name);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  @Override
  public A create(String type, String name) {
    T typeObject = getType(type);
    if (typeObject == null) {
      return null;
    } else {
      return create(typeObject, name);
    }
  }

  private T getType(String typeName) {
    for (T type : getTypes()) {
      if (typeName.equalsIgnoreCase(type.name())) {
        return type;
      }
    }
    return null;
  }

  protected abstract T[] getTypes();

}
