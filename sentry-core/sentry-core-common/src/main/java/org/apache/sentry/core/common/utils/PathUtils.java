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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class PathUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(PathUtils.class);
  private static String LOCAL_FILE_SCHEMA = "file";
  private static String AUTHORITY_PREFIX = "://";
  private static final Configuration CONF = new Configuration();

  @VisibleForTesting
  public static Configuration getConfiguration() {
    return CONF;
  }

  // TODO: "throws URISyntaxException" is kept for backward compatibility with the existing client code
  public static boolean impliesURI(URI privilegeURI, URI requestURI) throws URISyntaxException {
    return _impliesURI(privilegeURI.toString(), requestURI.toString());
  }

  /**
   * URI is a a special case. For URI's, /a implies /a/b.
   * Therefore the test is "/a/b".startsWith("/a");
   */
  private static boolean _impliesURI(String privilege, String request) {

    URI privilegeURI;
    URI requestURI;
    try {
      // build privilege URI, add default scheme and/or authority if missing
      privilegeURI = makeFullQualifiedURI(privilege);
      if (privilegeURI == null) {
        LOGGER.warn("Privilege URI " + privilege + " is not valid. Path is not absolute.");
        return false;
      }

      // build request URI, add default scheme and/or authority if missing
      requestURI = makeFullQualifiedURI(request);
      if (requestURI == null) {
        LOGGER.warn("Request URI " + request + " is not valid. Path is not absolute.");
        return false;
      }
    } catch (IOException e) {
      LOGGER.warn("Unable to get the configured filesystem implementation", e);
      return false;
    }

    // scheme and path must be present in privilege URI
    if (privilegeURI.getScheme() == null || privilegeURI.getPath() == null) {
      LOGGER.warn("Privilege URI " + request + " is not valid. Missing scheme or path.");
      return false;
    }

    // scheme and path must be present in request URI
    if (requestURI.getScheme() == null || requestURI.getPath() == null) {
      LOGGER.warn("Request URI " + request + " is not valid. Missing scheme or path.");
      return false;
    }

    // schemes in privilege and request URIs must be equal
    if (privilegeURI.getScheme() != null && !privilegeURI.getScheme().equals(requestURI.getScheme())) {
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

  /**
   * Make fully qualified URI if Scheme and/or Authority is missing,
   * based on the default file system Scheme and Authority.
   * Notes:
   * a) input URI path must be absolute; otherwise return null.
   * b) Path.makeQualified() provides no assurance that the
   *    default file system Scheme and Authority values are not null.
   *
   * @param uriName The Uri name.
   * @return Returns the fully qualified URI or null if URI path is not absolute.
   * @throws IOException
   */
  private static URI makeFullQualifiedURI(String uriName) throws IOException {
    Path uriPath = new Path(uriName);
    if (isNormalized(uriName) && uriPath.isUriPathAbsolute()) {
      // add scheme and/or authority if either is missing
      if ((uriPath.toUri().getScheme() == null || uriPath.toUri().getAuthority() == null)) {
        URI defaultUri = FileSystem.getDefaultUri(CONF);
        uriPath = uriPath.makeQualified(defaultUri, uriPath);
      }
      return uriPath.toUri();
    } else { // relative URI path is unacceptable
      return null;
    }
  }

  private static boolean isNormalized(String uriName) {
      URI uri = URI.create(uriName);
      return uri.getPath().equals(uri.normalize().getPath());
  }

  public static boolean impliesURI(String privilege, String request) {
    return _impliesURI(new StrSubstitutor(System.getProperties()).replace(privilege), request);
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
        uriAuthority = "";
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
