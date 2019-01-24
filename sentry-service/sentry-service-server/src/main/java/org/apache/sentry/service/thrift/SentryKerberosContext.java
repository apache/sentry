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

package org.apache.sentry.service.thrift;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class SentryKerberosContext implements Runnable {

  private static final String KERBEROS_RENEWER_THREAD_NAME = "kerberos-renewer-%d";
  private static final float TICKET_RENEW_WINDOW = 0.80f;
  private static final long CHECK_REFRESH_INTERVAL = 30L;
  private static final Logger LOGGER = LoggerFactory
      .getLogger(SentryKerberosContext.class);
  private LoginContext loginContext;
  private Subject subject;
  private final javax.security.auth.login.Configuration kerberosConfig;
  private ScheduledExecutorService renewerService;

  public SentryKerberosContext(String principal, String keyTab, boolean server)
      throws LoginException {
    subject = new Subject(false, Sets.newHashSet(new KerberosPrincipal(principal)),
          new HashSet<Object>(), new HashSet<Object>());
    if(server) {
      kerberosConfig = KerberosConfiguration.createServerConfig(principal, new File(keyTab));
    } else {
      kerberosConfig = KerberosConfiguration.createClientConfig(principal, new File(keyTab));
    }
    loginWithNewContext();
    if (!server) {
      startRenewerThread();
    }
  }

  private void loginWithNewContext() throws LoginException {
    LOGGER.info("Logging in with new Context");
    logoutSubject();
    loginContext = new LoginContext("", subject, null, kerberosConfig);
    loginContext.login();
    subject = loginContext.getSubject();
  }
  
  private void logoutSubject() {
    if (loginContext != null) {
      try {
        loginContext.logout();
      } catch (LoginException e) {
        LOGGER.warn("Error logging out the subject", e);
      }        
    }
    loginContext = null;
  }
  
  public Subject getSubject() {
    return subject;
  }

  /**
   * Get the Kerberos TGT
   * @return the user's TGT or null if none was found
   */
  private KerberosTicket getTGT() {
    Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
    for(KerberosTicket ticket: tickets) {
      KerberosPrincipal server = ticket.getServer();
      if (server.getName().equals("krbtgt/" + server.getRealm() +
          "@" + server.getRealm())) {
        return ticket;
      }
    }
    return null;
  }

  private long getRefreshTime(KerberosTicket tgt) {
    long start = tgt.getStartTime().getTime();
    long end = tgt.getEndTime().getTime();
    LOGGER.debug("Ticket start time: {}, end time: {}", start, end);
    return start + (long) ((end - start) * TICKET_RENEW_WINDOW);
  }

  /***
   * Ticket renewer thread
   * wait till 80% time interval left on the ticket and then renew it
   */
  @Override
  public void run() {
    try {
      KerberosTicket tgt = getTGT();
      if (tgt == null) {
        LOGGER.warn("No ticket found in the cache");
        return;
      }
      long nextRefresh = getRefreshTime(tgt);
      if (System.currentTimeMillis() >= nextRefresh) {
        loginWithNewContext();
        LOGGER.debug("Renewed ticket");
      }
    } catch (LoginException e) {
      LOGGER.warn("Failed to renew ticket", e);
    }
  }

  public void startRenewerThread() {
    ThreadFactory renewerThreadFactory = new ThreadFactoryBuilder()
            .setNameFormat(KERBEROS_RENEWER_THREAD_NAME)
            .build();
    renewerService = Executors.newSingleThreadScheduledExecutor(renewerThreadFactory);
    renewerService.scheduleWithFixedDelay(
            this, CHECK_REFRESH_INTERVAL, CHECK_REFRESH_INTERVAL, TimeUnit.SECONDS);
    LOGGER.info("Sentry Ticket renewer thread started");
  }

  public void shutDown() throws LoginException {
    if (renewerService != null) {
      renewerService.shutdownNow();
      LOGGER.info("Sentry Ticket renewer thread finished");
    }
    logoutSubject();
  }
}
