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

import org.apache.commons.lang.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class PathUtils {
  private static final Logger LOGGER = LoggerFactory
      .getLogger(PathUtils.class);
  /**
   * URI is a a special case. For URI's, /a implies /a/b.
   * Therefore the test is "/a/b".startsWith("/a");
   */
  public static boolean impliesURI(URI privilegeURI, URI requestURI)
    throws URISyntaxException {
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
    if (requestURI.getPath().equals(requestURI.normalize().getPath()) &&
        requestPath.startsWith(privilegePath) &&
        Strings.nullToEmpty(privilegeURI.getAuthority()).equals(Strings.nullToEmpty(requestURI.getAuthority()))) {
      return true;
    }
    return false;
  }

  public static boolean impliesURI(String privilege, String request) {
    try {
    URI privilegeURI = new URI(new StrSubstitutor(System.getProperties()).replace(privilege));
    URI requestURI = new URI(request);
    if(privilegeURI.getScheme() == null || privilegeURI.getPath() == null) {
      LOGGER.warn("Privilege URI " + request + " is not valid. Either no scheme or no path.");
      return false;
    }
    if(requestURI.getScheme() == null || requestURI.getPath() == null) {
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

  public static String parseDFSURI(String warehouseDir, String uri)
      throws URISyntaxException {
    return parseDFSURI(warehouseDir, uri, false);
  }

  /**
   * Parse a URI which should be on HDFS in the normal case but can be on a local
   * file system in the testing case. In either case it should be on the same fs
   * as the warehouse directory.
   */
  public static String parseDFSURI(String warehouseDir, String uri, boolean isLocal)
      throws URISyntaxException {
    if ((uri.startsWith("file://") || uri.startsWith("hdfs://"))) {
      return uri;
    } else {
      if (uri.startsWith("file:")) {
        uri = uri.replace("file:", "file://");
      } if (uri.startsWith("hdfs:")) {
        uri = uri.replace("hdfs:", "hdfs://");
      } else if (uri.startsWith("/")) {
        if (warehouseDir.startsWith("hdfs:")) {
          URI warehouse = toDFSURI(warehouseDir);
          uri = warehouse.getScheme() + "://" + warehouse.getAuthority() + uri;
        } else if (warehouseDir.startsWith("file:")) {
          uri = "file://" + uri;
        } else {
          if (isLocal) {
            uri = "file://" + uri;
          } else {
            // TODO fix this logic. I don't see why we would want to add hdfs://
            // to a URI at this point in time since no namenode is specified
            // and warehouseDir appear to just be a path starting with / ?
            // I think in the isLocal = false case we might want to throw
            uri = "hdfs://" + uri;
          }
        }
      }
      return uri;
    }
  }

  /**
   * Parse a URI which is on a local file system.
   */
  public static String parseLocalURI(String uri)
      throws URISyntaxException {
    if (uri.startsWith("file://")) {
      return uri;
    } else if (uri.startsWith("file:")) {
      return uri.replace("file:", "file://");
    } else if (uri.startsWith("/")) {
      return "file://" + uri;
    }
    throw new IllegalStateException("Parse URI does not work on relative URI: " + uri);
  }

  private static URI toDFSURI(String s) throws URISyntaxException {
    URI uri = new URI(s);
    if(uri.getScheme() == null || uri.getAuthority() == null) {
      throw new IllegalArgumentException("Invalid URI " + s + ". No scheme or authority.");
    }
    return uri;
  }
}
