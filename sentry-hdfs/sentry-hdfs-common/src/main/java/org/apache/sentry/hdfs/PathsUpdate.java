/**
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
import com.google.common.base.Preconditions;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.hdfs.service.thrift.TPathsUpdate;
import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;



/**
 * A wrapper class over the TPathsUpdate thrift generated class. Please see
 * {@link Updateable.Update} for more information
 */
public class PathsUpdate implements Updateable.Update {

  public static String ALL_PATHS = "__ALL_PATHS__";
  private static final Configuration CONF = new Configuration();
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

  @VisibleForTesting
  public static Configuration getConfiguration() {
    return CONF;
  }

  /**
   *
   * @param path : Needs to be a HDFS location with scheme
   * @return Path in the form a list containing the path tree with scheme/ authority stripped off.
   * Returns null if a non HDFS path or if path is null/empty
   */
  public static List<String> parsePath(String path) {
    try {

      URI uri = null;
      if (StringUtils.isNotEmpty(path)) {
        uri = new URI(URIUtil.encodePath(path));
      } else {
        return null;
      }

      String scheme = uri.getScheme();
      if (scheme == null) {
        // Use the default URI scheme only if the paths has no scheme.
        URI defaultUri = FileSystem.getDefaultUri(CONF);
        scheme = defaultUri.getScheme();
      }

      // The paths without a scheme will be default to default scheme.
      Preconditions.checkNotNull(scheme);

      // Non-HDFS paths will be skipped.
      if(scheme.equalsIgnoreCase("hdfs")) {
        return Lists.newArrayList(uri.getPath().split("^/")[1]
            .split("/"));
      } else {
        return null;
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException("Incomprehensible path [" + path + "]");
    } catch (URIException e){
      throw new RuntimeException("Unable to create URI: ",e);
    }
  }

  @Override
  public byte[] serialize() throws IOException {
    return ThriftSerializer.serialize(tPathsUpdate);
  }

  @Override
  public void deserialize(byte[] data) throws IOException {
    ThriftSerializer.deserialize(tPathsUpdate, data);
  }

}
