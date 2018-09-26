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
package org.apache.sentry.core.model.db;

public class Column implements DBModelAuthorizable {

  /**
   * Represents all Column
   */
  public static final Column ALL = new Column(AccessConstants.ALL);

  public static final Column SOME = new Column(AccessConstants.SOME);

  private final String name;

  public Column(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "Column [name=" + name + "]";
  }

  @Override
  public AuthorizableType getAuthzType() {
    return AuthorizableType.Column;
  }

  @Override
  public String getTypeName() {
    return getAuthzType().name();
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public boolean equals(Object o) {

    if(o == null) {
      return false;
    }

    if(!(o instanceof Column)) {
      return false;
    }

    if(((Column) o).getName() ==  null) {
      return false;
    }

    return ((Column) o).getName().equals(name);
  }
}
