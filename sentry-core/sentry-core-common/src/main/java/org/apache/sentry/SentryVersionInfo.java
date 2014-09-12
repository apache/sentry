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
package org.apache.sentry;

public class SentryVersionInfo {
  private static Package myPackage;
  private static SentryVersionAnnotation version;

  static {
    myPackage = SentryVersionAnnotation.class.getPackage();
    version = myPackage.getAnnotation(SentryVersionAnnotation.class);
  }

  /**
   * Get the meta-data for the Sentry package.
   * @return
   */
  static Package getPackage() {
    return myPackage;
  }

  /**
   * Get the Sentry version.
   * @return the Sentry version string, eg. "1.4.0-SNAPSHOT"
   */
  public static String getVersion() {
    return version != null ? version.version() : "Unknown";
  }

  /**
   * Get the subversion revision number for the root directory
   * @return the revision number, eg. "451451"
   */
  public static String getCommitHash() {
    return version != null ? version.commitHash() : "Unknown";
  }

  /**
   * Get the branch on which this originated.
   * @return The branch name, e.g. "master" or "branches/branch-1.4.0"
   */
  public static String getBranch() {
    return version != null ? version.branch() : "Unknown";
  }

  /**
   * The date that Sentry was compiled.
   * @return the compilation date in unix date format
   */
  public static String getDate() {
    return version != null ? version.date() : "Unknown";
  }

  /**
   * The user that compiled Sentry.
   * @return the username of the user
   */
  public static String getUser() {
    return version != null ? version.user() : "Unknown";
  }

  /**
   * Get the subversion URL for the root Sentry directory.
   */
  public static String getUrl() {
    return version != null ? version.url() : "Unknown";
  }

  /**
   * Get the checksum of the source files from which Sentry was built.
   **/
  public static String getSrcChecksum() {
    return version != null ? version.srcChecksum() : "Unknown";
  }

  /**
   * Returns the buildVersion which includes version, revision, user and date.
   */
  public static String getBuildVersion() {
    return "Apache Sentry " + SentryVersionInfo.getVersion()
        + " ,built from commit#" + SentryVersionInfo.getCommitHash()
        + " ,compiled by " + SentryVersionInfo.getUser()
        + " with source checksum "
        + SentryVersionInfo.getSrcChecksum();
  }

  public static void main(String[] args) {
    System.out.println("Sentry " + getVersion());
    System.out.println("Git " + getUrl());
    System.out.println("Commit# " + getCommitHash());
    System.out.println("Compiled by " + getUser() + " on " + getDate());
    System.out.println("From source with checksum " + getSrcChecksum());
  }
}
