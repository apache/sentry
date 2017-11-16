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
package org.apache.sentry.core.common.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is a simple publish-subscribe implementation for internal
 * communication between Sentry components. It's a singleton class.
 * <p>
 * For the initial set of use cases, publish events are expected to be
 * extremely rare, so no the data structures have been selected with no
 * consideration for high concurrency.
 */
public final class PubSub {

  private static final Logger LOGGER = LoggerFactory.getLogger(PubSub.class);

  private final Map<Topic,Set<Subscriber>> subscriberMap = new HashMap<>();

  private static PubSub instance;

  /**
   * Subscriber callback interface.
   * The same subscriber can subscribe to multiple topics,
   * so callback API includes both topic and message.
   */
  public interface Subscriber {
    void onMessage(Topic topic, String message);
  }

  /**
   * Enumerated topics one can subscribe to.
   * To be expanded as needed.
   */
  public enum Topic {
    HDFS_SYNC_HMS("hdfs-sync-hms"), // upcoming feature, triggering HDFS sync between HMS and Sentry
    HDFS_SYNC_NN("hdfs-sync-nn"); // upcoming feature, triggering HDFS sync between Sentry and NameNode

    private final String name;

    private static final Map<String,Topic> map = new HashMap<>();
    static {
      for (Topic t : Topic.values()) {
        map.put(t.name, t);
      }
    }

    public static Topic fromString(String name) {
      Preconditions.checkNotNull("Enum name cannot be null", name);
      name = name.trim().toLowerCase();
      if (map.containsKey(name)) {
        return map.get(name);
      }
      throw new NoSuchElementException(name + " not found");
    }

    private Topic(String name) {
      this.name = name.toLowerCase();
    }

    public String getName() {
      return name;
    }
  }

  /**
   * Public factory method to guarantee singleton
   */
  public static synchronized PubSub getInstance() {
    if (instance != null) {
      LOGGER.info(instance + " requested");
    } else {
      instance = new PubSub();
      LOGGER.info(instance + " created");
    }
    return instance;
  }

  // declare private to prevent multiple class instantiation
  private PubSub() {
  }

  /**
   * Publish message on given topic. Message is optional.
   */
  public synchronized void publish(Topic topic, String message) {
    Preconditions.checkNotNull(topic, "Topic cannot be null");

    Set<Subscriber> subscribers = subscriberMap.get(topic);
    if (subscribers == null) {
      throw new IllegalArgumentException("cannot publish to unknown topic " + topic
        + ", existing topics " + subscriberMap.keySet());
    }

    for (Subscriber subscriber : subscribers) {
      // Faulire of one subscriber to process message delivery should not affect
      // message delivery to other subscribers, therefore using try-catch.
      try {
        subscriber.onMessage(topic, message);
      } catch (Exception e) {
        LOGGER.error("Topic " + topic + ", message " + message + ", delivery error", e);
      }
    }
    LOGGER.info("Topic " + topic + ", message " + message + ": " + subscribers.size() + " subscribers called");
  }

  /**
   * Subscribe to given topic.
   */
  public synchronized void subscribe(Topic topic, Subscriber subscriber) {
    Preconditions.checkNotNull(topic, "Topic cannot be null");
    Preconditions.checkNotNull(subscriber, "Topic %s: Subscriber cannot be null", topic);

    Set<Subscriber> subscribers = subscriberMap.get(topic);
    if (subscribers == null) {
      LOGGER.info("new topic " + topic);
      subscriberMap.put(topic, subscribers = new HashSet<Subscriber>());
    }
    subscribers.add(subscriber);
    LOGGER.info("Topic " + topic + ", added subscriber " + subscriber + ", total topic subscribers: " + subscribers.size());
  }

  /**
   * Unsubscribe from given topic. If the last subscriber, remove the topic.
   */
  public synchronized void unsubscribe(Topic topic, Subscriber subscriber) {
    Preconditions.checkNotNull(topic, "Topic cannot be null");
    Preconditions.checkNotNull(subscriber, "Topic %s: Subscriber cannot be null", topic);

    Set<Subscriber> subscribers = subscriberMap.get(topic);
    if (subscribers == null) {
      throw new IllegalArgumentException("cannot unsubscribe from unknown topic " + topic);
    }
    if (!subscribers.remove(subscriber)) {
      throw new IllegalArgumentException("cannot unsubscribe from topic " + topic + ", unknown subscriber");
    }
    LOGGER.info("Topic " + topic + ", unsubscribing subscriber " + subscriber + ", total topic subscribers: " + subscribers.size());
    if (subscribers.isEmpty()) {
      subscriberMap.remove(topic);
    }
  }

  /**
   * Get all existing topics.
   */
  public synchronized Set<Topic> getTopics() {
    return  subscriberMap.keySet();
  }
  
  @Override
  public String toString() {
    return getClass().getSimpleName() + ":" + Integer.toHexString(hashCode());
  }
}
