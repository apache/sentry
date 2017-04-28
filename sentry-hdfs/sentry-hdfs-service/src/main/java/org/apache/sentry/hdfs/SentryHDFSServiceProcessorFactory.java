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

import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.hdfs.service.thrift.SentryHDFSService;
import org.apache.sentry.hdfs.service.thrift.SentryHDFSService.Iface;
import org.apache.sentry.provider.db.service.persistent.SentryStore;
import org.apache.sentry.core.common.utils.ThriftUtil;
import org.apache.sentry.service.thrift.ProcessorFactory;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SentryHDFSServiceProcessorFactory extends ProcessorFactory{

  private static final Logger LOGGER = LoggerFactory.getLogger(SentryHDFSServiceProcessorFactory.class);

  static class ProcessorWrapper extends SentryHDFSService.Processor<SentryHDFSService.Iface> {

    public ProcessorWrapper(Iface iface) {
      super(iface);
    }
    @Override
    public boolean process(TProtocol in, TProtocol out) throws TException {
      ThriftUtil.setIpAddress(in);
      ThriftUtil.setImpersonator(in);
      return super.process(in, out);
    }
  }

  public SentryHDFSServiceProcessorFactory(Configuration conf) {
    super(conf);
  }

  @Override
  public boolean register(TMultiplexedProcessor multiplexedProcessor,
                          SentryStore _) throws Exception {
    SentryHDFSServiceProcessor sentryServiceHandler =
        new SentryHDFSServiceProcessor();
    LOGGER.info("Calling registerProcessor from SentryHDFSServiceProcessorFactory");
    TProcessor processor = new ProcessorWrapper(sentryServiceHandler);
    multiplexedProcessor.registerProcessor(
        SentryHDFSServiceClient.SENTRY_HDFS_SERVICE_NAME, processor);
    return true;
  }
}
