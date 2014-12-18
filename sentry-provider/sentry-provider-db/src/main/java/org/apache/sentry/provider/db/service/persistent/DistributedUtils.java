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

package org.apache.sentry.provider.db.service.persistent;

import org.apache.hadoop.conf.Configuration;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class DistributedUtils {

  public static final String SENTRY_DISTRIBUTED_TOPIC = "sentryDistributedTopic";
  public static final String SENTRY_CATCHUP_REQUEST_TOPIC = "sentryCatchUpRequestTopic";
  public static final String SENTRY_CATCHUP_RESPONSE_QUEUE = "sentryCatchUpResponseQueue_";
  
  public static final String SENTRY_STORE_SEQID = "sentryStoreSeqId";
  public static final String SENTRY_STORE_NODEID = "sentryStoreNodeId";

  public static String SENTRY_CATCHUP_WAIT_TIME = "sentry.catchup.wait.time";
  public static int SENTRY_CATCHUP_WAIT_TIME_DEF = 10000;

  public static String SENTRY_HAZELCAST_PORT_START = "sentry.hazelcast.port.start";
  public static int SENTRY_HAZELCAST_PORT_START_DEF = 9000;
  public static String SENTRY_HAZELCAST_MEMBERS = "sentry.hazelcast.members";
  public static String[] SENTRY_HAZELCAST_MEMBERS_DEF = new String[]{"localhost"};
  public static String SENTRY_HAZELCAST_INSTANCE_GROUP = "sentry.hazelcast.instance.group";
  public static String SENTRY_HAZELCAST_INSTANCE_GROUP_DEF = "sentry";
  public static String SENTRY_HAZELCAST_INSTANCE_PASSWORD = "sentry.hazelcast.instance.password";
  public static String SENTRY_HAZELCAST_INSTANCE_PASSWORD_DEF = "sentry";

  public static HazelcastInstance getHazelcastInstance(Configuration conf,
      boolean createNew) {
    Config cfg = new Config();
    NetworkConfig netCfg = cfg.getNetworkConfig();
    netCfg.setPort(conf.getInt(SENTRY_HAZELCAST_PORT_START,
        SENTRY_HAZELCAST_PORT_START_DEF));
    netCfg.setPortAutoIncrement(true);
    JoinConfig joinCfg = netCfg.getJoin();
    joinCfg.getMulticastConfig().setEnabled(false);
    TcpIpConfig tcpIpConfig = joinCfg.getTcpIpConfig();
    for (String member : conf.getStrings(SENTRY_HAZELCAST_MEMBERS,
        SENTRY_HAZELCAST_MEMBERS_DEF)) {
      tcpIpConfig.addMember(member);
    }
    tcpIpConfig.setEnabled(true);

    cfg.addTopicConfig(new TopicConfig()
        .setGlobalOrderingEnabled(true)
        .setName(SENTRY_DISTRIBUTED_TOPIC));
    cfg.getGroupConfig().setName(
        conf.get(SENTRY_HAZELCAST_INSTANCE_GROUP,
            SENTRY_HAZELCAST_INSTANCE_GROUP_DEF));
    cfg.getGroupConfig().setPassword(
        conf.get(SENTRY_HAZELCAST_INSTANCE_PASSWORD,
            SENTRY_HAZELCAST_INSTANCE_PASSWORD_DEF));
    if (createNew) {
      return Hazelcast.newHazelcastInstance(cfg);
    } else {
      return Hazelcast.getOrCreateHazelcastInstance(cfg);
    }
  }

}
