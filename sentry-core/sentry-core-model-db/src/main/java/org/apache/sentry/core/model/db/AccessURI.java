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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

public class AccessURI implements DBModelAuthorizable {
  /**
   * Represents all URIs
   */
  public static final AccessURI ALL = new AccessURI(AccessConstants.ALL);
  private static final String AUTHORITY_PREFIX = "://";

  private final String uriName;

  /**
   * Wrap a URI which can be HDFS, S3, SWIFT, WEBHDFS,etc. Do the validation for the URI's format.
   */
  public AccessURI(String uriName) {
    uriName = uriName == null ? "" : uriName;

    // Validating the URI format
    if (!uriName.equals(AccessConstants.ALL)) {
      Path uriPath = new Path(uriName);
      String schema = uriPath.toUri().getScheme();
      if (StringUtils.isBlank(schema) || !uriPath.isAbsolute()) {
        throw new IllegalArgumentException("URI '" + uriName
            + "' is invalid. Unsupported URI without schema or relative URI.");
      }

      if (!uriName.startsWith(schema + AUTHORITY_PREFIX)) {
        throw new IllegalArgumentException("URI '" + uriName + "' is invalid.");
      }
    }

    // ALL(*) represents all URIs.
    this.uriName = uriName;
  }

  @Override
  public String getName() {
    return uriName;
  }

  @Override
  public AuthorizableType getAuthzType() {
    return AuthorizableType.URI;
  }

  @Override
  public String toString() {
    return "URI [name=" + uriName + "]";
  }

  @Override
  public String getTypeName() {
    return getAuthzType().name();
  }

  @Override
  public int hashCode() {
    return uriName.hashCode();
  }

  @Override
  public boolean equals(Object o) {

    if(o == null) {
      return false;
    }

    if(!(o instanceof AccessURI)) {
      return false;
    }

    if(((AccessURI) o).getName() ==  null) {
      return false;
    }

    return ((AccessURI) o).getName().equals(uriName);
  }
}
