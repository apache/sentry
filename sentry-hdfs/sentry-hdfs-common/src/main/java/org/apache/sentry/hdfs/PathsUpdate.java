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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.sentry.hdfs.service.thrift.TPathChanges;
import org.apache.sentry.hdfs.service.thrift.TPathsUpdate;

import com.google.common.collect.Lists;

/**
 * A wrapper class over the TPathsUpdate thrift generated class. Please see
 * {@link Updateable.Update} for more information 
 */
public class PathsUpdate implements Updateable.Update {
  
  public static String ALL_PATHS = "__ALL_PATHS__";

  private final TPathsUpdate tPathsUpdate;

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
   *
   * @param path : Needs to be a HDFS location with scheme
   * @return Path in the form a list containing the path tree with scheme/ authority stripped off.
   * Returns null if a non HDFS path
   */
  public static List<String> parsePath(String path) {
    try {
      URI uri = new URI(path);
      Preconditions.checkNotNull(uri.getScheme());
      if(uri.getScheme().equalsIgnoreCase("hdfs")) {
        return Lists.newArrayList(uri.getPath().split("^/")[1]
            .split("/"));
      } else {
        return null;
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException("Incomprehensible path [" + path + "]");
    }
  }

}
