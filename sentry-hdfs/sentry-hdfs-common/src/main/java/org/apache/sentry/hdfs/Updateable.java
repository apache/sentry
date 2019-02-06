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

import org.apache.thrift.TException;

import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;

public interface Updateable<K extends Updateable.Update> {

  /**
   * Thrift currently does not support class inheritance. We need all update
   * objects to expose a unified API. A wrapper class need to be created
   * implementing this interface and containing the generated thrift class as
   * a work around
   */
  interface Update {

    boolean hasFullImage();

    long getSeqNum();

    void setSeqNum(long seqNum);

    long getImgNum();

    void setImgNum(long imgNum);

    byte[] serialize() throws IOException;

    void deserialize(byte data[]) throws IOException;

    /**
     *  Deserialize the update from a JSON format representation to an object.
     *
     * @param update the given JSON string representation
     * @throws TException
     */
    void JSONDeserialize(String update) throws TException;

    /**
     * Serialize the update from the object to a JSON format representation.
     *
     * @return the JSON string representation
     * @throws TException
     */
    String JSONSerialize() throws TException;
  }

  /**
   * Apply multiple partial updates in order
   * @param update
   * @param lock External Lock.
   * @return
   */
  void updatePartial(Iterable<K> update, ReadWriteLock lock);

  /**
   * This returns a new object with the full update applied
   * @param update
   * @return
   */
  Updateable<K> updateFull(K update);

  /**
   * Return sequence number of Last Update
   */
  long getLastUpdatedSeqNum();

  /**
   * Return image number of Last Update
   * @return
   */
  long getLastUpdatedImgNum();

  /**
   * Create and Full image update of the local data structure
   * @param currSeqNum
   * @return
   */
  K createFullImageUpdate(long currSeqNum) throws Exception;

  String getUpdateableTypeName();

  /**
  Get the string with the sequence and image number.
  */
  String getSequenceInfo();
}
