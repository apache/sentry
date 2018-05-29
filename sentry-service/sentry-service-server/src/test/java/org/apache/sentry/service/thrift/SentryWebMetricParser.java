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
package org.apache.sentry.service.thrift;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;


/**
 * The SentryWebMetricParser is used to parse the metrics displayed on the sentry web ui.
 */
public class SentryWebMetricParser {
  private JsonNode root;
  private final String sentryService = SentryService.class.getName();
  private ObjectMapper mapper;

  public SentryWebMetricParser(String response) throws IOException {
    this.mapper = new ObjectMapper();
    this.root = mapper.readTree(response);
  }

  public void refreshRoot(String response) throws IOException {
    root = mapper.readTree(response);
  }

  public JsonNode getRoot() {
    return root;
  }

  public JsonNode getGauges(JsonNode root) {
    JsonNode gauges = root.findPath("gauges");
    return gauges;
  }

  public JsonNode getCounters(JsonNode root) {
    JsonNode counters = root.findPath("counters");
    return counters;
  }

  public JsonNode getHistograms(JsonNode root) {
    JsonNode histograms = root.findPath("histograms");
    return histograms;
  }

  public JsonNode getMeters(JsonNode root) {
    JsonNode meters = root.findPath("meters");
    return meters;
  }

  public JsonNode getTimers(JsonNode root) {
    JsonNode timers = root.findPath("timers");
    return timers;
  }

  public JsonNode getValue(JsonNode node) {
    return node.findPath("value");
  }

  public boolean isHA() {
    JsonNode gauges = getGauges(root);
    JsonNode obj = getValue(gauges.findPath(sentryService + ".is_ha"));
    return obj.getValueAsBoolean();
  }

  public boolean isActive() {
    JsonNode gauges = getGauges(root);
    JsonNode obj = getValue(gauges.findPath(sentryService + ".is_active"));
    return obj.getBooleanValue();
  }
}