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

import org.apache.sentry.core.common.utils.PubSub;
import org.apache.sentry.core.common.utils.PubSub.Topic;
import org.apache.sentry.service.thrift.SentryServiceIntegrationBase;

import org.junit.*;

import java.net.HttpURLConnection;
import java.net.URL;

public class TestSentryServerPubSub extends SentryServiceIntegrationBase {

  private static final Topic[] topics = Topic.values();
  private static final String[] messages = { "message1", "message2", "message3", "" };

  private static volatile String REQUEST_URL;

  private final TestSubscriber testSubscriber = new TestSubscriber();

  private static final class TestSubscriber implements PubSub.Subscriber {
    private volatile Topic topic;
    private volatile String message;
    private volatile int count;
    @Override
    public void onMessage(Topic topic, String message) {
      this.topic = topic;
      this.message = message;
      this.count++;
    }
  }

  @BeforeClass
  public static void setup() throws Exception {
    webServerEnabled = true;
    webSecurity = false;
    SentryServiceIntegrationBase.setup();
    REQUEST_URL= "http://" + SERVER_HOST + ":" + webServerPort + "/admin/publishMessage?topic=%s&message=%s";
  }

  @Override
  @Before
  public void before() throws Exception {

    // Subscribe  to all defined topics.
    // After each successfull HTTP-GET, testSubscriber.onMessage()
    // will be called and "topic" and "message" fields will be
    // set according to HTTP-GET parameters.
    testSubscriber.count = 0;
    for (Topic topic : topics) {
      PubSub.getInstance().subscribe(topic, testSubscriber);
    }
    Assert.assertEquals("Unexpected number of registered topics", topics.length, PubSub.getInstance().getTopics().size());
  }

  @Override
  @After
  public void after() {
    // unsubscribe
    for (Topic topic : topics) {
      PubSub.getInstance().unsubscribe(topic, testSubscriber);
    }
    testSubscriber.count = 0;
    Assert.assertTrue("Topics should have been removed after unsubscribe()", PubSub.getInstance().getTopics().isEmpty());
  }

  /**
   * Successfully publish notifications
   * @throws Exception
   */
  @Test
  public void testPubSub() throws Exception {
    int count = 0;
    for (Topic topic : topics) {
      for (String message : messages) {
        URL url = new URL(String.format(REQUEST_URL, topic.getName(), message));
        HttpURLConnection conn = null;
        try {
          conn = (HttpURLConnection) url.openConnection();
          Assert.assertEquals("Unexpected response code", HttpURLConnection.HTTP_OK, conn.getResponseCode());
        } finally {
          safeClose(conn);
        }
        Assert.assertEquals("Unexpected topic", topic, testSubscriber.topic);
        if (message.isEmpty()) {
          Assert.assertEquals("Unexpected message", null, testSubscriber.message);
        } else {
          Assert.assertEquals("Unexpected message", message, testSubscriber.message);
        }
        Assert.assertEquals("Unexpected number of PubSub.onMessage() callbacks", ++count, testSubscriber.count);
      }
    }
  }

  /**
   * Submit empty topic. It's ok, generates form page.
   * @throws Exception
   */
  @Test
  public void testPubSubEmptyTopic() throws Exception {
    URL url = new URL(String.format(REQUEST_URL, "", "message"));
    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) url.openConnection();
      Assert.assertEquals("Unexpected response code", HttpURLConnection.HTTP_OK,  conn.getResponseCode());
    } finally {
      safeClose(conn);
    }
    Assert.assertEquals("Unexpected number of PubSub.onMessage() callbacks", 0, testSubscriber.count);
  }

  /**
   * Submit invalid topic
   * @throws Exception
   */
  @Test
  public void testPubSubInvalidTopic() throws Exception {
    String[] invalid_topics = { "invalid_topic_1", "invalid_topic_2", "invalid_topic_3" };
    for (String topic : invalid_topics) {
      URL url = new URL(String.format(REQUEST_URL, topic, "message"));
      HttpURLConnection conn = null;
      try {
        conn = (HttpURLConnection) url.openConnection();
        Assert.assertEquals("Unexpected response code", HttpURLConnection.HTTP_BAD_REQUEST,  conn.getResponseCode());
      } finally {
        safeClose(conn);
      }
      Assert.assertEquals("Unexpected number of PubSub.onMessage() callbacks", 0, testSubscriber.count);
    }
  }

  /**
   * Submit topic that has no subscribers.
   * @throws Exception
   */
  @Test
  public void testPubSubNonSubscribedTopic() throws Exception {
    // At this point all valid Topic values have been subscribed to
    // in before() method.
    // Unsubscribe from one topic and then try publishing to it.
    PubSub.getInstance().unsubscribe(Topic.HDFS_SYNC_HMS, testSubscriber);
    Assert.assertEquals("Unexpected number of registered topics", topics.length-1, PubSub.getInstance().getTopics().size());

    URL url = new URL(String.format(REQUEST_URL, Topic.HDFS_SYNC_HMS.getName(), "message"));
    HttpURLConnection conn = null;
    try {
      conn = (HttpURLConnection) url.openConnection();
      Assert.assertEquals("Unexpected response code", HttpURLConnection.HTTP_BAD_REQUEST,  conn.getResponseCode());
    } finally {
      safeClose(conn);
    }
    // re-subscribe, not to upset after() method which expects all topics to be subscribed to
    PubSub.getInstance().subscribe(Topic.HDFS_SYNC_HMS, testSubscriber);
  }

  private static void safeClose(HttpURLConnection conn) {
    if (conn != null) {
      try {
        conn.disconnect();
      } catch (Exception ignore) {
      }
    }
  }
}
