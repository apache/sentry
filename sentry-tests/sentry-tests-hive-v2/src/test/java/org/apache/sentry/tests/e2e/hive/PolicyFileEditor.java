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

package org.apache.sentry.tests.e2e.hive;




import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * Deprecated} use Context.append()
 */
public class PolicyFileEditor {

  private static final String NL = System.getProperty("line.separator", "\n");

  private File policy;

  public PolicyFileEditor (File policy) throws IOException {
    policy.delete();
    policy.createNewFile();
    this.policy = policy;
  }

  public void clearOldPolicy() throws IOException {
    policy.delete();
    policy.createNewFile();
  }

  public void addPolicy(String line, String cat) throws IOException {
    List<String> result = new ArrayList<String>();
    boolean exist = false;
    for(String s : Files.readLines(policy, Charsets.UTF_8)) {
      result.add(s);
      if (s.equals("[" + cat + "]")) {
        result.add(line);
        exist = true;
       }
    }
    if (!exist) {
      result.add("[" + cat + "]");
      result.add(line);
    }
    Files.write(Joiner.on(NL).join(result), policy, Charsets.UTF_8);
  }
  public void removePolicy(String line) throws IOException {
    List<String> result = Lists.newArrayList();
    for(String s : Files.readLines(policy, Charsets.UTF_8)) {
      if (!s.equals(line)) {
        result.add(s);
      }
    }
    Files.write(Joiner.on(NL).join(result), policy, Charsets.UTF_8);
  }
}
