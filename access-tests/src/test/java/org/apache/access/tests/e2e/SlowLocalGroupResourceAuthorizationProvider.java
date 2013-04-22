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

package org.apache.access.tests.e2e;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.access.core.Action;
import org.apache.access.core.Authorizable;
import org.apache.access.core.Subject;
import org.apache.access.provider.file.LocalGroupResourceAuthorizationProvider;

public class SlowLocalGroupResourceAuthorizationProvider extends LocalGroupResourceAuthorizationProvider {

  static long sleepLengthSeconds = 30;
  private boolean hasSlept;

  public SlowLocalGroupResourceAuthorizationProvider(String resource, String serverName) throws IOException {
    super(resource, serverName);
  }
  @Override
  public boolean hasAccess(Subject subject, List<Authorizable> authorizableHierarchy,
      EnumSet<Action> actions) {
    if(!hasSlept) {
      hasSlept = true;
      try {
        TimeUnit.SECONDS.sleep(sleepLengthSeconds);
      } catch (InterruptedException exception) {
        throw new RuntimeException(exception);
      }
    }
    return super.hasAccess(subject, authorizableHierarchy, actions);
  }

  static void setSleepLengthInSeconds(long seconds) {
    sleepLengthSeconds = seconds;
  }
}
