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

package org.apache.sentry.hdfs;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import junit.framework.Assert;

import org.apache.sentry.hdfs.UpdateForwarder;
import org.apache.sentry.hdfs.Updateable;
import org.apache.sentry.hdfs.UpdateForwarder.ExternalImageRetriever;
import org.apache.sentry.hdfs.Updateable.Update;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class TestUpdateForwarder {
  
  static class DummyUpdate implements Update {
    private long seqNum = 0;
    private boolean hasFullUpdate = false;
    private String stuff;
    public DummyUpdate(long seqNum, boolean hasFullUpdate) {
      this.seqNum = seqNum;
      this.hasFullUpdate = hasFullUpdate;
    }
    public String getStuff() {
      return stuff;
    }
    public DummyUpdate setStuff(String stuff) {
      this.stuff = stuff;
      return this;
    }
    @Override
    public boolean hasFullImage() {
      return hasFullUpdate;
    }
    @Override
    public long getSeqNum() {
      return seqNum;
    }
    @Override
    public void setSeqNum(long seqNum) {
     this.seqNum = seqNum;
    }
  }

  static class DummyUpdatable implements Updateable<DummyUpdate> {
    
    private List<String> state = new LinkedList<String>();
    private long lastUpdatedSeqNum = 0;

    @Override
    public void updatePartial(Iterable<DummyUpdate> update, ReadWriteLock lock) {
      for (DummyUpdate u : update) {
        state.add(u.getStuff());
        lastUpdatedSeqNum = u.seqNum;
      }
    }

    @Override
    public Updateable<DummyUpdate> updateFull(DummyUpdate update) {
      DummyUpdatable retVal = new DummyUpdatable();
      retVal.lastUpdatedSeqNum = update.seqNum;
      retVal.state = Lists.newArrayList(update.stuff.split(","));
      return retVal;
    }

    @Override
    public long getLastUpdatedSeqNum() {
      return lastUpdatedSeqNum;
    }

    @Override
    public DummyUpdate createFullImageUpdate(long currSeqNum) {
      DummyUpdate retVal = new DummyUpdate(currSeqNum, true);
      retVal.stuff = Joiner.on(",").join(state);
      return retVal;
    }

    public String getState() {
      return Joiner.on(",").join(state);
    }
  }

  static class DummyImageRetreiver implements ExternalImageRetriever<DummyUpdate> {

    private String state;
    public void setState(String state) {
      this.state = state;
    }
    @Override
    public DummyUpdate retrieveFullImage(long currSeqNum) {
      DummyUpdate retVal = new DummyUpdate(currSeqNum, true);
      retVal.stuff = state;
      return retVal;
    }
  }

  @Test
  public void testInit() {
    DummyImageRetreiver imageRetreiver = new DummyImageRetreiver();
    imageRetreiver.setState("a,b,c");
    UpdateForwarder<DummyUpdate> updateForwarder = new UpdateForwarder<DummyUpdate>(
        new DummyUpdatable(), imageRetreiver, 10);
    Assert.assertEquals(-2, updateForwarder.getLastUpdatedSeqNum());
    List<DummyUpdate> allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertTrue(allUpdates.size() == 1);
    Assert.assertEquals("a,b,c", allUpdates.get(0).getStuff());

    // If the current process has restarted the input seqNum will be > currSeq
    allUpdates = updateForwarder.getAllUpdatesFrom(100);
    Assert.assertTrue(allUpdates.size() == 1);
    Assert.assertEquals("a,b,c", allUpdates.get(0).getStuff());
    Assert.assertEquals(-2, allUpdates.get(0).getSeqNum());
    allUpdates = updateForwarder.getAllUpdatesFrom(-1);
    Assert.assertEquals(0, allUpdates.size());
  }

  @Test
  public void testUpdateReceive() throws Exception {
    DummyImageRetreiver imageRetreiver = new DummyImageRetreiver();
    imageRetreiver.setState("a,b,c");
    UpdateForwarder<DummyUpdate> updateForwarder = new UpdateForwarder<DummyUpdate>(
        new DummyUpdatable(), imageRetreiver, 5);
    updateForwarder.handleUpdateNotification(new DummyUpdate(5, false).setStuff("d"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(5, updateForwarder.getLastUpdatedSeqNum());
    List<DummyUpdate> allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertEquals(2, allUpdates.size());
    Assert.assertEquals("a,b,c", allUpdates.get(0).getStuff());
    Assert.assertEquals("d", allUpdates.get(1).getStuff());
  }

  @Test
  public void testGetUpdates() throws Exception {
    DummyImageRetreiver imageRetreiver = new DummyImageRetreiver();
    imageRetreiver.setState("a,b,c");
    UpdateForwarder<DummyUpdate> updateForwarder = new UpdateForwarder<DummyUpdate>(
        new DummyUpdatable(), imageRetreiver, 5);
    updateForwarder.handleUpdateNotification(new DummyUpdate(5, false).setStuff("d"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(5, updateForwarder.getLastUpdatedSeqNum());
    List<DummyUpdate> allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertEquals(2, allUpdates.size());

    updateForwarder.handleUpdateNotification(new DummyUpdate(6, false).setStuff("e"));
    updateForwarder.handleUpdateNotification(new DummyUpdate(7, false).setStuff("f"));

    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(7, updateForwarder.getLastUpdatedSeqNum());
    allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertEquals(4, allUpdates.size());
    Assert.assertEquals("a,b,c", allUpdates.get(0).getStuff());
    Assert.assertEquals(4, allUpdates.get(0).getSeqNum());
    Assert.assertEquals("d", allUpdates.get(1).getStuff());
    Assert.assertEquals(5, allUpdates.get(1).getSeqNum());
    Assert.assertEquals("e", allUpdates.get(2).getStuff());
    Assert.assertEquals(6, allUpdates.get(2).getSeqNum());
    Assert.assertEquals("f", allUpdates.get(3).getStuff());
    Assert.assertEquals(7, allUpdates.get(3).getSeqNum());

    updateForwarder.handleUpdateNotification(new DummyUpdate(8, false).setStuff("g"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(8, updateForwarder.getLastUpdatedSeqNum());
    allUpdates = updateForwarder.getAllUpdatesFrom(8);
    Assert.assertEquals(1, allUpdates.size());
    Assert.assertEquals("g", allUpdates.get(0).getStuff());
  }

  @Test
  public void testGetUpdatesAfterExternalEntityReset() throws Exception {
    DummyImageRetreiver imageRetreiver = new DummyImageRetreiver();
    imageRetreiver.setState("a,b,c");
    UpdateForwarder<DummyUpdate> updateForwarder = new UpdateForwarder<DummyUpdate>(
        new DummyUpdatable(), imageRetreiver, 5);
    updateForwarder.handleUpdateNotification(new DummyUpdate(5, false).setStuff("d"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }

    updateForwarder.handleUpdateNotification(new DummyUpdate(6, false).setStuff("e"));
    updateForwarder.handleUpdateNotification(new DummyUpdate(7, false).setStuff("f"));

    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(7, updateForwarder.getLastUpdatedSeqNum());
    List<DummyUpdate> allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertEquals(4, allUpdates.size());
    Assert.assertEquals("f", allUpdates.get(3).getStuff());
    Assert.assertEquals(7, allUpdates.get(3).getSeqNum());

    updateForwarder.handleUpdateNotification(new DummyUpdate(8, false).setStuff("g"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(8, updateForwarder.getLastUpdatedSeqNum());
    allUpdates = updateForwarder.getAllUpdatesFrom(8);
    Assert.assertEquals(1, allUpdates.size());
    Assert.assertEquals("g", allUpdates.get(0).getStuff());

    imageRetreiver.setState("a,b,c,d,e,f,g,h");

    // New update comes with SeqNum = 1
    updateForwarder.handleUpdateNotification(new DummyUpdate(1, false).setStuff("h"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    // NN plugin asks for next update
    allUpdates = updateForwarder.getAllUpdatesFrom(9);
    Assert.assertEquals(1, allUpdates.size());
    Assert.assertEquals("a,b,c,d,e,f,g,h", allUpdates.get(0).getStuff());
    Assert.assertEquals(1, allUpdates.get(0).getSeqNum());
  }

  @Test
  public void testUpdateLogCompression() throws Exception {
    DummyImageRetreiver imageRetreiver = new DummyImageRetreiver();
    imageRetreiver.setState("a,b,c");
    UpdateForwarder<DummyUpdate> updateForwarder = new UpdateForwarder<DummyUpdate>(
        new DummyUpdatable(), imageRetreiver, 5);
    updateForwarder.handleUpdateNotification(new DummyUpdate(5, false).setStuff("d"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(5, updateForwarder.getLastUpdatedSeqNum());
    List<DummyUpdate> allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertEquals(2, allUpdates.size());

    updateForwarder.handleUpdateNotification(new DummyUpdate(6, false).setStuff("e"));
    updateForwarder.handleUpdateNotification(new DummyUpdate(7, false).setStuff("f"));
    updateForwarder.handleUpdateNotification(new DummyUpdate(8, false).setStuff("g"));
    updateForwarder.handleUpdateNotification(new DummyUpdate(9, false).setStuff("h"));
    updateForwarder.handleUpdateNotification(new DummyUpdate(10, false).setStuff("i"));
    updateForwarder.handleUpdateNotification(new DummyUpdate(11, false).setStuff("j"));

    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(11, updateForwarder.getLastUpdatedSeqNum());
    allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertEquals(3, allUpdates.size());
    Assert.assertEquals("a,b,c,d,e,f,g,h", allUpdates.get(0).getStuff());
    Assert.assertEquals(9, allUpdates.get(0).getSeqNum());
    Assert.assertEquals("i", allUpdates.get(1).getStuff());
    Assert.assertEquals(10, allUpdates.get(1).getSeqNum());
    Assert.assertEquals("j", allUpdates.get(2).getStuff());
    Assert.assertEquals(11, allUpdates.get(2).getSeqNum());
  }
}
