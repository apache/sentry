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

package org.apache.sentry.core.model.sqoop;

import junit.framework.Assert;

import org.apache.sentry.core.model.sqoop.Connector;
import org.apache.sentry.core.model.sqoop.Job;
import org.apache.sentry.core.model.sqoop.Link;
import org.apache.sentry.core.model.sqoop.Server;
import org.apache.sentry.core.model.sqoop.SqoopAuthorizable.AuthorizableType;
import org.junit.Test;

public class TestSqoopAuthorizable {

  @Test
  public void testSimpleName() throws Exception {
    String name = "simple";
    Server server = new Server(name);
    Assert.assertEquals(server.getName(), name);

    Connector connector = new Connector(name);
    Assert.assertEquals(connector.getName(), name);

    Link link = new Link(name);
    Assert.assertEquals(link.getName(), name);

    Job job = new Job(name);
    Assert.assertEquals(job.getName(), name);
  }

  @Test
  public void testAuthType() throws Exception {
    Server server = new Server("server1");
    Assert.assertEquals(server.getAuthzType(), AuthorizableType.SERVER);

    Connector connector = new Connector("connector1");
    Assert.assertEquals(connector.getAuthzType(), AuthorizableType.CONNECTOR);

    Link link = new Link("link1");
    Assert.assertEquals(link.getAuthzType(), AuthorizableType.LINK);

    Job job = new Job("job1");
    Assert.assertEquals(job.getAuthzType(), AuthorizableType.JOB);
  }
}
