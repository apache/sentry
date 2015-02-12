package org.apache.sentry.core.indexer;
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

import junit.framework.Assert;

import org.apache.sentry.core.model.indexer.Indexer;
import org.junit.Test;

public class TestIndexer {

  @Test
  public void testSimple() {
    String name = "simple";
    Indexer simple = new Indexer(name);
    Assert.assertEquals(simple.getName(), name);
  }

  @Test
  public void testIndexerAuthzType() {
    Indexer indexer1 = new Indexer("indexer1");
    Indexer indexer2 = new Indexer("indexer2");
    Assert.assertEquals(indexer1.getAuthzType(), indexer2.getAuthzType());
    Assert.assertEquals(indexer1.getTypeName(), indexer2.getTypeName());
  }

  // just test it doesn't throw NPE
  @Test
  public void testNullIndexer() {
    Indexer nullIndexer = new Indexer(null);
    nullIndexer.getName();
    nullIndexer.toString();
    nullIndexer.getAuthzType();
    nullIndexer.getTypeName();
  }
}
