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

import com.google.common.annotations.VisibleForTesting;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TJSONProtocol;

public class ThriftSerializer {

  final static private TJSONProtocol.Factory tJSONProtocol =
          new TJSONProtocol.Factory();

  // Use default max thrift message size here.
  // TODO: Figure out a way to make maxMessageSize configurable, eg. create a serializer singleton at startup by
  // passing a max_size parameter
  @VisibleForTesting
  static long maxMessageSize = ServiceConstants.ClientConfig.SENTRY_HDFS_THRIFT_MAX_MESSAGE_SIZE_DEFAULT;

  @SuppressWarnings("rawtypes")
  public static byte[] serialize(TBase baseObject) throws IOException {
    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory(maxMessageSize, maxMessageSize));
    try {
      return serializer.serialize(baseObject);
    } catch (TException e) {
      throw new IOException("Error serializing thrift object "
          + baseObject, e);
    }
  }

  @SuppressWarnings("rawtypes")
  public static TBase deserialize(TBase baseObject, byte[] serialized) throws IOException {
    TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory(maxMessageSize, maxMessageSize));
    try {
      deserializer.deserialize(baseObject, serialized);
    } catch (TException e) {
      throw new IOException("Error deserializing thrift object "
          + baseObject, e);
    }
    return baseObject;
  }

  public static String serializeToJSON(TBase base) throws TException  {
    // Initiate a new TSerializer each time for thread safety.
    TSerializer tSerializer = new TSerializer(tJSONProtocol);
    return tSerializer.toString(base);
  }

  public static void deserializeFromJSON(TBase base, String dataInJson) throws TException {
    // Initiate a new TDeserializer each time for thread safety.
    TDeserializer tDeserializer = new TDeserializer(tJSONProtocol);
    tDeserializer.fromString(base, dataInJson);
  }

}
