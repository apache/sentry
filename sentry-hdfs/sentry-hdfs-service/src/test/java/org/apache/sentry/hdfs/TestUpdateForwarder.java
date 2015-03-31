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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.hdfs.UpdateForwarder;
import org.apache.sentry.hdfs.Updateable;
import org.apache.sentry.hdfs.UpdateForwarder.ExternalImageRetriever;
import org.apache.sentry.hdfs.Updateable.Update;
import org.apache.sentry.service.thrift.ServiceConstants.ServerConfig;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class TestUpdateForwarder {

  public static class DummyUpdate implements Update {
    private long seqNum = 0;
    private boolean hasFullUpdate = false;
    private String state;
    public DummyUpdate() {
      this(0, false);
    }
    public DummyUpdate(long seqNum, boolean hasFullUpdate) {
      this.seqNum = seqNum;
      this.hasFullUpdate = hasFullUpdate;
    }
    public String getState() {
      return state;
    }
    public DummyUpdate setState(String stuff) {
      this.state = stuff;
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
    @Override
    public byte[] serialize() throws IOException {
      return state.getBytes();
    }

    @Override
    public void deserialize(byte[] data) throws IOException {
      state = new String(data);
    }
  }

  static class DummyUpdatable implements Updateable<DummyUpdate> {

    private List<String> state = new LinkedList<String>();
    private long lastUpdatedSeqNum = 0;

    @Override
    public void updatePartial(Iterable<DummyUpdate> update, ReadWriteLock lock) {
      for (DummyUpdate u : update) {
        state.add(u.getState());
        lastUpdatedSeqNum = u.seqNum;
      }
    }

    @Override
    public Updateable<DummyUpdate> updateFull(DummyUpdate update) {
      DummyUpdatable retVal = new DummyUpdatable();
      retVal.lastUpdatedSeqNum = update.seqNum;
      retVal.state = Lists.newArrayList(update.state.split(","));
      return retVal;
    }

    @Override
    public long getLastUpdatedSeqNum() {
      return lastUpdatedSeqNum;
    }

    @Override
    public DummyUpdate createFullImageUpdate(long currSeqNum) {
      DummyUpdate retVal = new DummyUpdate(currSeqNum, true);
      retVal.state = Joiner.on(",").join(state);
      return retVal;
    }

    public String getState() {
      return Joiner.on(",").join(state);
    }

    @Override
    public String getUpdateableTypeName() {
      // TODO Auto-generated method stub
      return "DummyUpdator";
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
      retVal.state = state;
      return retVal;
    }
  }

  protected Configuration testConf = new Configuration();
  protected UpdateForwarder<DummyUpdate> updateForwarder;

  @After
  public void cleanup() throws Exception {
    if (updateForwarder != null) {
      updateForwarder.close();
      updateForwarder = null;
    }
  }

  @Test
  public void testInit() throws Exception {
    DummyImageRetreiver imageRetreiver = new DummyImageRetreiver();
    imageRetreiver.setState("a,b,c");
    updateForwarder = UpdateForwarder.create(
        testConf, new DummyUpdatable(), new DummyUpdate(), imageRetreiver, 10);
    Assert.assertEquals(-2, updateForwarder.getLastUpdatedSeqNum());
    List<DummyUpdate> allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertTrue(allUpdates.size() == 1);
    Assert.assertEquals("a,b,c", allUpdates.get(0).getState());

    // If the current process has restarted the input seqNum will be > currSeq
    allUpdates = updateForwarder.getAllUpdatesFrom(100);
    Assert.assertTrue(allUpdates.size() == 1);
    Assert.assertEquals("a,b,c", allUpdates.get(0).getState());
    Assert.assertEquals(-2, allUpdates.get(0).getSeqNum());
    allUpdates = updateForwarder.getAllUpdatesFrom(-1);
    Assert.assertEquals(0, allUpdates.size());
  }

  @Test
  public void testUpdateReceive() throws Exception {
    DummyImageRetreiver imageRetreiver = new DummyImageRetreiver();
    imageRetreiver.setState("a,b,c");
    updateForwarder = UpdateForwarder.create(
        testConf, new DummyUpdatable(), new DummyUpdate(), imageRetreiver, 5);
    updateForwarder.handleUpdateNotification(new DummyUpdate(5, false).setState("d"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(5, updateForwarder.getLastUpdatedSeqNum());
    List<DummyUpdate> allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertEquals(2, allUpdates.size());
    Assert.assertEquals("a,b,c", allUpdates.get(0).getState());
    Assert.assertEquals("d", allUpdates.get(1).getState());
  }

  // This happens when we the first update from HMS is a -1 (If the heartbeat
  // thread checks Sentry's current seqNum before any update has come in)..
  // This will lead the first and second entries in the updatelog to differ
  // by more than +1..
  @Test
  public void testUpdateReceiveWithNullImageRetriver() throws Exception {
    Assume.assumeTrue(!testConf.getBoolean(ServerConfig.SENTRY_HA_ENABLED,
        false));
    updateForwarder = UpdateForwarder.create(
        testConf, new DummyUpdatable(), new DummyUpdate(), null, 5);
    updateForwarder.handleUpdateNotification(new DummyUpdate(-1, true).setState("a"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    List<DummyUpdate> allUpdates = updateForwarder.getAllUpdatesFrom(1);
    Assert.assertEquals("a", allUpdates.get(0).getState());
    updateForwarder.handleUpdateNotification(new DummyUpdate(6, false).setState("b"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    updateForwarder.handleUpdateNotification(new DummyUpdate(7, false).setState("c"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(7, updateForwarder.getLastUpdatedSeqNum());
    allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertEquals(2, allUpdates.size());
    Assert.assertEquals("b", allUpdates.get(0).getState());
    Assert.assertEquals("c", allUpdates.get(1).getState());
  }

  @Test
  public void testGetUpdates() throws Exception {
    DummyImageRetreiver imageRetreiver = new DummyImageRetreiver();
    imageRetreiver.setState("a,b,c");
    updateForwarder = UpdateForwarder.create(
        testConf, new DummyUpdatable(), new DummyUpdate(), imageRetreiver, 5);
    updateForwarder.handleUpdateNotification(new DummyUpdate(5, false).setState("d"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(5, updateForwarder.getLastUpdatedSeqNum());
    List<DummyUpdate> allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertEquals(2, allUpdates.size());

    updateForwarder.handleUpdateNotification(new DummyUpdate(6, false).setState("e"));
    updateForwarder.handleUpdateNotification(new DummyUpdate(7, false).setState("f"));

    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(7, updateForwarder.getLastUpdatedSeqNum());
    allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertEquals(4, allUpdates.size());
    Assert.assertEquals("a,b,c", allUpdates.get(0).getState());
    Assert.assertEquals(4, allUpdates.get(0).getSeqNum());
    Assert.assertEquals("d", allUpdates.get(1).getState());
    Assert.assertEquals(5, allUpdates.get(1).getSeqNum());
    Assert.assertEquals("e", allUpdates.get(2).getState());
    Assert.assertEquals(6, allUpdates.get(2).getSeqNum());
    Assert.assertEquals("f", allUpdates.get(3).getState());
    Assert.assertEquals(7, allUpdates.get(3).getSeqNum());

    updateForwarder.handleUpdateNotification(new DummyUpdate(8, false).setState("g"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(8, updateForwarder.getLastUpdatedSeqNum());
    allUpdates = updateForwarder.getAllUpdatesFrom(8);
    Assert.assertEquals(1, allUpdates.size());
    Assert.assertEquals("g", allUpdates.get(0).getState());
  }

  @Test
  public void testGetUpdatesAfterExternalEntityReset() throws Exception {
    /*
     * Disabled for Sentry HA. Since the sequence numbers are trakced in ZK, the
     * lower sequence updates are ignored which causes this test to fail in HA
     * mode
     */
    Assume.assumeTrue(!testConf.getBoolean(ServerConfig.SENTRY_HA_ENABLED,
        false));

    DummyImageRetreiver imageRetreiver = new DummyImageRetreiver();
    imageRetreiver.setState("a,b,c");
    updateForwarder = UpdateForwarder.create(
        testConf, new DummyUpdatable(), new DummyUpdate(), imageRetreiver, 5);
    updateForwarder.handleUpdateNotification(new DummyUpdate(5, false).setState("d"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }

    updateForwarder.handleUpdateNotification(new DummyUpdate(6, false).setState("e"));
    updateForwarder.handleUpdateNotification(new DummyUpdate(7, false).setState("f"));

    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(7, updateForwarder.getLastUpdatedSeqNum());
    List<DummyUpdate> allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertEquals(4, allUpdates.size());
    Assert.assertEquals("f", allUpdates.get(3).getState());
    Assert.assertEquals(7, allUpdates.get(3).getSeqNum());

    updateForwarder.handleUpdateNotification(new DummyUpdate(8, false).setState("g"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(8, updateForwarder.getLastUpdatedSeqNum());
    allUpdates = updateForwarder.getAllUpdatesFrom(8);
    Assert.assertEquals(1, allUpdates.size());
    Assert.assertEquals("g", allUpdates.get(0).getState());

    imageRetreiver.setState("a,b,c,d,e,f,g,h");

    // New update comes with SeqNum = 1
    updateForwarder.handleUpdateNotification(new DummyUpdate(1, false).setState("h"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    // NN plugin asks for next update
    allUpdates = updateForwarder.getAllUpdatesFrom(9);
    Assert.assertEquals(1, allUpdates.size());
    Assert.assertEquals("a,b,c,d,e,f,g,h", allUpdates.get(0).getState());
    // Assert.assertEquals(1, allUpdates.get(0).getSeqNum());
  }

  @Test
  public void testUpdateLogCompression() throws Exception {
    DummyImageRetreiver imageRetreiver = new DummyImageRetreiver();
    imageRetreiver.setState("a,b,c");
    updateForwarder = UpdateForwarder.create(
        testConf, new DummyUpdatable(), new DummyUpdate(), imageRetreiver, 5);
    updateForwarder.handleUpdateNotification(new DummyUpdate(5, false).setState("d"));
    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(5, updateForwarder.getLastUpdatedSeqNum());
    List<DummyUpdate> allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertEquals(2, allUpdates.size());

    updateForwarder.handleUpdateNotification(new DummyUpdate(6, false).setState("e"));
    updateForwarder.handleUpdateNotification(new DummyUpdate(7, false).setState("f"));
    updateForwarder.handleUpdateNotification(new DummyUpdate(8, false).setState("g"));
    updateForwarder.handleUpdateNotification(new DummyUpdate(9, false).setState("h"));
    updateForwarder.handleUpdateNotification(new DummyUpdate(10, false).setState("i"));
    updateForwarder.handleUpdateNotification(new DummyUpdate(11, false).setState("j"));

    while(!updateForwarder.areAllUpdatesCommited()) {
      Thread.sleep(100);
    }
    Assert.assertEquals(11, updateForwarder.getLastUpdatedSeqNum());
    allUpdates = updateForwarder.getAllUpdatesFrom(0);
    Assert.assertEquals(3, allUpdates.size());
    Assert.assertEquals("a,b,c,d,e,f,g,h", allUpdates.get(0).getState());
    Assert.assertEquals(9, allUpdates.get(0).getSeqNum());
    Assert.assertEquals("i", allUpdates.get(1).getState());
    Assert.assertEquals(10, allUpdates.get(1).getSeqNum());
    Assert.assertEquals("j", allUpdates.get(2).getState());
    Assert.assertEquals(11, allUpdates.get(2).getSeqNum());
  }
}
