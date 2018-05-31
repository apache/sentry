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

package org.apache.sentry.provider.db.service.model;

import javax.jdo.annotations.PersistenceCapable;

@PersistenceCapable
public class MSentryVersion {
  private String schemaVersion;
  private String versionComment;

  public MSentryVersion() {
  }

  public MSentryVersion(String schemaVersion, String versionComment) {
    this.schemaVersion = schemaVersion.intern();
    this.versionComment = versionComment.intern();
  }

  /**
   * @return the versionComment
   */
  public String getVersionComment() {
    return versionComment;
  }

  /**
   * @param versionComment
   *          the versionComment to set
   */
  public void setVersionComment(String versionComment) {
    this.versionComment = versionComment;
  }

  /**
   * @return the schemaVersion
   */
  public String getSchemaVersion() {
    return schemaVersion;
  }

  /**
   * @param schemaVersion
   *          the schemaVersion to set
   */
  public void setSchemaVersion(String schemaVersion) {
    this.schemaVersion = schemaVersion;
  }

}
