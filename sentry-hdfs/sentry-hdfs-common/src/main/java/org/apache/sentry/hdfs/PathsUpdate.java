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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sentry.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.hdfs.service.thrift.TPathsUpdate;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

import org.apache.thrift.TException;

/**
 * A wrapper class over the TPathsUpdate thrift generated class. Please see
 * {@link Updateable.Update} for more information
 */
public class PathsUpdate implements Updateable.Update {

  public static final String ALL_PATHS = "__ALL_PATHS__";


  private static final Configuration CONF = new Configuration();
  private static String DEFAULT_SCHEME = FileSystem.getDefaultUri(CONF).getScheme();
  private static final String SUPPORTED_SCHEME = "hdfs";

  private final TPathsUpdate tPathsUpdate;

  public PathsUpdate() {
    this(0, false);
  }

  public PathsUpdate(TPathsUpdate tPathsUpdate) {
    this.tPathsUpdate = tPathsUpdate;
  }

  public PathsUpdate(long seqNum, boolean hasFullImage) {
    tPathsUpdate = new TPathsUpdate(hasFullImage, seqNum,
        new LinkedList<TPathChanges>());
  }

  @Override
  public boolean hasFullImage() {
    return tPathsUpdate.isHasFullImage();
  }

  public TPathChanges newPathChange(String authzObject) {

    TPathChanges pathChanges = new TPathChanges(authzObject,
        new LinkedList<List<String>>(), new LinkedList<List<String>>());
    tPathsUpdate.addToPathChanges(pathChanges);
    return pathChanges;
  }

  public List<TPathChanges> getPathChanges() {
    return tPathsUpdate.getPathChanges();
  }

  @Override
  public long getSeqNum() {
    return tPathsUpdate.getSeqNum();
  }

  @Override
  public void setSeqNum(long seqNum) {
    tPathsUpdate.setSeqNum(seqNum);
  }

  public TPathsUpdate toThrift() {
    return tPathsUpdate;
  }

  /**
   * Only used for testing.
   * @param scheme new default scheme
   */
  @VisibleForTesting
  public static void setDefaultScheme(String scheme) {
    DEFAULT_SCHEME = scheme;
  }

  /**
   * Convert URI to path, trimming leading slash.
   * @param path HDFS location in one of the forms:
   * <ul>
   *   <li>hdfs://hostname:port/path
   *   <li>hdfs:///path
   *   <li>/path, in which case, scheme will be constructed from FileSystem.getDefaultURI
   *   <li>URIs with non hdfs schemee will just be ignored
   * </ul>
   * @return Path with scheme/ authority stripped off.
   * Returns null if a non HDFS path or if path is null/empty.
   */
  public static String parsePath(String path) throws SentryMalformedPathException {
    if (StringUtils.isEmpty(path)) {
      return null;
    }

    URI uri;
    try {
      uri = new URI(URIUtil.encodePath(path));
    } catch (URISyntaxException e) {
      throw new SentryMalformedPathException("Incomprehensible path [" + path + "]", e);
    } catch (URIException e) {
      throw new SentryMalformedPathException("Unable to create URI from path[" + path + "]", e);
    }

    String scheme = uri.getScheme();
    if (scheme == null) {
      scheme = DEFAULT_SCHEME;
      if(scheme == null) {
        throw new SentryMalformedPathException(
                "Scheme is missing and could not be constructed from configuration");
      }
    }

    // Non-HDFS paths are skipped.
    if(!scheme.equalsIgnoreCase(SUPPORTED_SCHEME)) {
      return null;
    }

    String uriPath = uri.getPath();
    if(uriPath == null) {
      throw new SentryMalformedPathException("Path is empty. uri=" + uri);
    }
    if (!uriPath.startsWith("/")) {
      throw new SentryMalformedPathException("Path part of uri does not seem right, was expecting a non empty path" +
              ": path = " + uriPath + ", uri=" + uri);
    }
    // Remove leading slash
    return uriPath.substring(1);
  }

  @Override
  public byte[] serialize() throws IOException {
    return ThriftSerializer.serialize(tPathsUpdate);
  }

  @Override
  public void deserialize(byte[] data) throws IOException {
    ThriftSerializer.deserialize(tPathsUpdate, data);
  }

  @Override
  public void JSONDeserialize(String update) throws TException {
    ThriftSerializer.deserializeFromJSON(tPathsUpdate, update);
  }

  @Override
  public String JSONSerialize() throws TException {
    return ThriftSerializer.serializeToJSON(tPathsUpdate);
  }

  @Override
  public int hashCode() {
    return (tPathsUpdate == null) ? 0 : tPathsUpdate.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }

    if (this == obj) {
      return true;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    PathsUpdate other = (PathsUpdate) obj;
    if (tPathsUpdate == null) {
      return other.tPathsUpdate == null;
    }
    return tPathsUpdate.equals(other.tPathsUpdate);
  }

}
