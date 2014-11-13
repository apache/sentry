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
package org.apache.sentry.core.common.utils;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class PathUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PathUtils.class);
  private static String LOCAL_FILE_SCHEMA = "file";
  private static String AUTHORITY_PREFIX = "://";

  /**
   * URI is a a special case. For URI's, /a implies /a/b.
   * Therefore the test is "/a/b".startsWith("/a");
   */
  public static boolean impliesURI(URI privilegeURI, URI requestURI) throws URISyntaxException {
    if (privilegeURI.getPath() == null || requestURI.getPath() == null) {
      return false;
    }
    // ensure that either both schemes are null or equal
    if (privilegeURI.getScheme() == null) {
      if (requestURI.getScheme() != null) {
        return false;
      }
    } else if (!privilegeURI.getScheme().equals(requestURI.getScheme())) {
      return false;
    }
    // request path does not contain relative parts /a/../b &&
    // request path starts with privilege path &&
    // authorities (nullable) are equal
    String requestPath = ensureEndsWithSeparator(requestURI.getPath()).replace("//", "/");
    String privilegePath = ensureEndsWithSeparator(privilegeURI.getPath()).replace("//", "/");
    if (requestURI.getPath().equals(requestURI.normalize().getPath())
        && requestPath.startsWith(privilegePath)
        && Strings.nullToEmpty(privilegeURI.getAuthority()).equals(
            Strings.nullToEmpty(requestURI.getAuthority()))) {
      return true;
    }
    return false;
  }

  public static boolean impliesURI(String privilege, String request) {
    try {
      URI privilegeURI = new URI(new StrSubstitutor(System.getProperties()).replace(privilege));
      URI requestURI = new URI(request);
      if (privilegeURI.getScheme() == null || privilegeURI.getPath() == null) {
        LOGGER.warn("Privilege URI " + request + " is not valid. Either no scheme or no path.");
        return false;
      }
      if (requestURI.getScheme() == null || requestURI.getPath() == null) {
        LOGGER.warn("Request URI " + request + " is not valid. Either no scheme or no path.");
        return false;
      }
      return PathUtils.impliesURI(privilegeURI, requestURI);
    } catch (URISyntaxException e) {
      LOGGER.warn("Request URI " + request + " is not a URI", e);
      return false;
    }
  }

  /**
   * The URI must be a directory as opposed to a partial
   * path entry name. To ensure this is true we add a /
   * at the end of the path. Without this the admin might
   * grant access to /dir1 but the user would be given access
   * to /dir1* whereas the admin meant /dir1/
   */
  private static String ensureEndsWithSeparator(String path) {
    if (path.endsWith(File.separator)) {
      return path;
    }
    return path + File.separator;
  }

  public static String parseDFSURI(String warehouseDir, String uri) throws URISyntaxException {
    return parseURI(warehouseDir, uri, false);
  }

  /**
   * Parse a URI which can be HDFS, S3, SWIFT, WEBHDFS,etc. In either case it
   * should be on the same fs as the warehouse directory.
   */
  public static String parseURI(String warehouseDir, String uri, boolean isLocal)
      throws URISyntaxException {
    Path warehouseDirPath = new Path(warehouseDir);
    Path uriPath = new Path(uri);

    if (uriPath.isAbsolute()) {
      // Merge warehouseDir and uri only when there is no scheme and authority
      // in uri.
      if (uriPath.isAbsoluteAndSchemeAuthorityNull()) {
        uriPath = uriPath.makeQualified(warehouseDirPath.toUri(), warehouseDirPath);
      }
      String uriScheme = uriPath.toUri().getScheme();
      String uriAuthority = uriPath.toUri().getAuthority();

      if (StringUtils.isEmpty(uriScheme) || isLocal) {
        uriScheme = LOCAL_FILE_SCHEMA;
      }

      uriPath = new Path(uriScheme + AUTHORITY_PREFIX + StringUtils.trimToEmpty(uriAuthority)
          + Path.getPathWithoutSchemeAndAuthority(uriPath));
    } else {
      // don't support relative path
      throw new IllegalArgumentException("Invalid URI " + uri + ".");
    }
    return uriPath.toUri().toString();
  }

  /**
   * Parse a URI which is on a local file system.
   */
  public static String parseLocalURI(String uri)
      throws URISyntaxException {
    Path uriPath = new Path(uri);
    if (uriPath.isAbsolute()) {
      uriPath = new Path(LOCAL_FILE_SCHEMA + AUTHORITY_PREFIX
          + StringUtils.trimToEmpty(uriPath.toUri().getAuthority())
          + Path.getPathWithoutSchemeAndAuthority(uriPath));
    } else {
      throw new IllegalArgumentException("Parse URI does not work on relative URI: " + uri);
    }
    return uriPath.toUri().toString();
  }

}
