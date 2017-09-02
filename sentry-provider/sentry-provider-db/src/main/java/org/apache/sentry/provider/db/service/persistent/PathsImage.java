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

package org.apache.sentry.provider.db.service.persistent;

import java.util.Collection;
import java.util.Map;

/**
 * A container for complete hive paths snapshot.
 * <p>
 * It is composed by a hiveObj to Paths mapping, a paths image ID and the sequence number/change ID
 * of latest delta change that the snapshot maps to.
 */
public class PathsImage {

  // A full image of hiveObj to Paths mapping.
  private final Map<String, Collection<String>> pathImage;
  private final long id;
  private final long curImgNum;

  public PathsImage(Map<String, Collection<String>> pathImage, long id, long curImgNum) {
    this.pathImage = pathImage;
    this.id = id;
    this.curImgNum = curImgNum;
  }

  public long getId() {
    return id;
  }

  public long getCurImgNum() {
    return curImgNum;
  }

  public Map<String, Collection<String>> getPathImage() {
    return pathImage;
  }
}
