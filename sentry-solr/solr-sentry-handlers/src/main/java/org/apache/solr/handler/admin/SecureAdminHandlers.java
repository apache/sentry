/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.admin;

import java.io.IOException;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.sentry.SecureRequestHandlerUtil;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.zookeeper.KeeperException;

/**
 * Secure version of AdminHandlers that creates Sentry-aware AdminHandlers.
 */
public class SecureAdminHandlers extends AdminHandlers {

  protected static class StandardHandler {
    private final String name;
    private final SolrRequestHandler handler;
    
    public StandardHandler( String n, SolrRequestHandler h )
    {
      this.name = n;
      this.handler = h;
    }
  }

  /**
   * NOTE: ideally we'd just override the list of admin handlers, but
   * since AdminHandlers in solr doesn't allow it, let's just copy the
   * entire function.  This is deprecated in Solr 5.0, so we should do something
   * different with Solr 5.0 anyway.
   */
  @Override
  public void inform(SolrCore core) 
  {
    String path = null;
    for( Map.Entry<String, SolrRequestHandler> entry : core.getRequestHandlers().entrySet() ) {
      if( entry.getValue() == this ) {
        path = entry.getKey();
        break;
      }
    }
    if( path == null ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
          "The AdminHandler is not registered with the current core." );
    }
    if( !path.startsWith( "/" ) ) {
      throw new SolrException( SolrException.ErrorCode.SERVER_ERROR, 
        "The AdminHandler needs to be registered to a path.  Typically this is '/admin'" );
    }
    // Remove the parent handler 
    core.registerRequestHandler(path, null);
    if( !path.endsWith( "/" ) ) {
      path += "/";
    }
    
    StandardHandler[] list = new StandardHandler[] {
       new StandardHandler( "luke", new SecureLukeRequestHandler() ),
       new StandardHandler( "system", new SecureSystemInfoHandler() ),
       new StandardHandler( "mbeans", new SecureSolrInfoMBeanHandler() ),
       new StandardHandler( "plugins", new SecurePluginInfoHandler() ),
       new StandardHandler( "threads", new SecureThreadDumpHandler() ),
       new StandardHandler( "properties", new SecurePropertiesRequestHandler() ),
       new StandardHandler( "logging", new SecureLoggingHandler() ),
       new StandardHandler( "file", new SecureShowFileRequestHandler() )
    };
    
    for( StandardHandler handler : list ) {
      if( core.getRequestHandler( path+handler.name ) == null ) {
        handler.handler.init( initArgs );
        core.registerRequestHandler( path+handler.name, handler.handler );
        if( handler.handler instanceof SolrCoreAware ) {
          ((SolrCoreAware)handler.handler).inform(core);
        }
      }
    }
  }

  public static class SecureLoggingHandler extends LoggingHandler {
    public SecureLoggingHandler(CoreContainer cc) {
      super(cc);
    }

    public SecureLoggingHandler() {
      super();
    }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      // logging handler can be used both to read and change logs
      SecureRequestHandlerUtil.checkSentryAdmin(req, SecureRequestHandlerUtil.QUERY_AND_UPDATE, getClass().getName(), false, null);
      super.handleRequestBody(req, rsp);
    }
  }

  public static class SecureLukeRequestHandler extends LukeRequestHandler {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      SecureRequestHandlerUtil.checkSentryAdmin(req, SecureRequestHandlerUtil.QUERY_ONLY, getClass().getName(), true, null);
      super.handleRequestBody(req, rsp);
    }
  }

  public static class SecurePluginInfoHandler extends PluginInfoHandler {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      SecureRequestHandlerUtil.checkSentryAdmin(req, SecureRequestHandlerUtil.QUERY_ONLY, getClass().getName(), true, null);
      super.handleRequestBody(req, rsp);
    }
  }

  public static class SecurePropertiesRequestHandler extends PropertiesRequestHandler {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
      SecureRequestHandlerUtil.checkSentryAdmin(req, SecureRequestHandlerUtil.QUERY_ONLY, getClass().getName(), false, null);
      super.handleRequestBody(req, rsp);
    }
  }

  public static class SecureShowFileRequestHandler extends ShowFileRequestHandler {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp)
     throws IOException, KeeperException, InterruptedException {
      SecureRequestHandlerUtil.checkSentryAdmin(req, SecureRequestHandlerUtil.QUERY_ONLY, getClass().getName(), true, null);
      super.handleRequestBody(req, rsp);
    }
  }

  public static class SecureSolrInfoMBeanHandler extends SolrInfoMBeanHandler {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      SecureRequestHandlerUtil.checkSentryAdmin(req, SecureRequestHandlerUtil.QUERY_ONLY, getClass().getName(), true, null);
      super.handleRequestBody(req, rsp);
    }
  }

  public static class SecureSystemInfoHandler extends SystemInfoHandler {
    public SecureSystemInfoHandler() {
      super();
    }

    public SecureSystemInfoHandler(CoreContainer cc) {
      super(cc);
    }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      // this may or may not have the core
      SolrCore core = req.getCore();
      SecureRequestHandlerUtil.checkSentryAdmin(req, SecureRequestHandlerUtil.QUERY_ONLY, getClass().getName(), core != null, null);
      super.handleRequestBody(req, rsp);
    }
  }

  public static class SecureThreadDumpHandler extends ThreadDumpHandler {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {
      SecureRequestHandlerUtil.checkSentryAdmin(req, SecureRequestHandlerUtil.QUERY_ONLY, getClass().getName(), false, null);
      super.handleRequestBody(req, rsp);
    }
  }
}
