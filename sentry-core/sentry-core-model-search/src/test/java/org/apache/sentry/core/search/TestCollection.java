package org.apache.sentry.core.search;
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

import org.apache.sentry.core.model.search.Collection;
import org.junit.Test;

public class TestCollection {

  @Test
  public void testSimple() {
    String name = "simple";
    Collection simple = new Collection(name);
    Assert.assertEquals(simple.getName(), name);
  }

  @Test
  public void testCollectionAuthzType() {
    Collection collection1 = new Collection("collection1");
    Collection collection2 = new Collection("collection2");
    Assert.assertEquals(collection1.getAuthzType(), collection2.getAuthzType());
    Assert.assertEquals(collection1.getTypeName(), collection2.getTypeName());
  }

  // just test it doesn't throw NPE
  @Test
  public void testNullCollection() {
    Collection nullCollection = new Collection(null);
    nullCollection.getName();
    nullCollection.toString();
    nullCollection.getAuthzType();
    nullCollection.getTypeName();
  }
}
