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

package org.apache.sentry.provider.db.service.thrift;

import static org.junit.Assert.assertTrue;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import javax.security.auth.Subject;

import org.apache.sentry.SentryUserException;
import org.apache.sentry.service.thrift.SentryServiceFactory;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestSentryServiceClientPool extends SentryServiceIntegrationBase {

  @Test
  public void testConnectionWhenReconnect() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        String roleName = "admin_r";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        client.dropRoleIfExists(requestorUserName, roleName);
        client.createRole(requestorUserName, roleName);
        client.listRoles(requestorUserName);
        stopSentryService();
        server = new SentryServiceFactory().create(conf);
        startSentryService();
        client.listRoles(requestorUserName);
        client.dropRole(requestorUserName, roleName);
      }
    });
  }

  @Test
  public void testConnectionWithMultipleRetries() throws Exception {
    runTestAsSubject(new TestOperation() {
      @Override
      public void runTestAsSubject() throws Exception {
        List<Future<Boolean>> tasks = new ArrayList<Future<Boolean>>();
        String requestorUserName = ADMIN_USER;
        Set<String> requestorUserGroupNames = Sets.newHashSet(ADMIN_GROUP);
        String roleName = "admin_r";
        setLocalGroupMapping(requestorUserName, requestorUserGroupNames);
        writePolicyFile();

        client.dropRoleIfExists(requestorUserName, roleName);
        client.createRole(requestorUserName, roleName);

        ExecutorService executorService = Executors.newFixedThreadPool(20);

        Callable<Boolean> func = new Callable<Boolean>() {
          public Boolean call() throws Exception {
            return Subject.doAs(clientSubject, new PrivilegedExceptionAction<Boolean>() {
              @Override
              public Boolean run() throws Exception {
                try {
                  client.listRoles(ADMIN_USER);
                  return true;
                } catch (SentryUserException sue) {
                  return false;
                }
              }
            });
          }
        };

        for (int i = 0; i < 30; i++) {
          FutureTask<Boolean> task = new FutureTask<Boolean>(func);
          tasks.add(task);
          executorService.submit(task);
        }

        for (Future<Boolean> task : tasks) {
          Boolean result = task.get();
          assertTrue("Some tasks are failed.", result);
        }
      }
    });
  }
}