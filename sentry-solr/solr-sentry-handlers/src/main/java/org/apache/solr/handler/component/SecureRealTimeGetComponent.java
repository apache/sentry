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

package org.apache.solr.handler.component;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.transform.DocTransformer;
import org.apache.solr.response.transform.DocTransformers;
import org.apache.solr.response.transform.TransformContext;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SolrReturnFields;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.sentry.SentryIndexAuthorizationSingleton;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.RefCounted;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public class SecureRealTimeGetComponent extends SearchComponent
{
  private static Logger log =
    LoggerFactory.getLogger(SecureRealTimeGetComponent.class);
  public static String ID_FIELD_NAME = "_reserved_sentry_id";
  public static final String COMPONENT_NAME = "secureGet";

  private SentryIndexAuthorizationSingleton sentryInstance;

  public SecureRealTimeGetComponent() {
    this(SentryIndexAuthorizationSingleton.getInstance());
  }

  @VisibleForTesting
  public SecureRealTimeGetComponent(SentryIndexAuthorizationSingleton sentryInstance) {
    super();
    this.sentryInstance = sentryInstance;
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    QueryDocAuthorizationComponent docComponent =
        (QueryDocAuthorizationComponent)rb.req.getCore().getSearchComponent("queryDocAuthorization");
    if (docComponent != null) {
      String userName = sentryInstance.getUserName(rb.req);
      String superUser = (System.getProperty("solr.authorization.superuser", "solr"));
      // security is never applied to the super user; for example, if solr internally is using
      // real time get for replica synchronization, we need to return all the documents.
      if (docComponent.getEnabled() && !superUser.equals(userName)) {
        Set<String> roles = sentryInstance.getRoles(userName);
        if (roles != null && roles.size() > 0) {
          SolrReturnFields savedReturnFields = (SolrReturnFields)rb.rsp.getReturnFields();
          if (savedReturnFields == null) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Not able to authorize request because ReturnFields is invalid: " + savedReturnFields);
          }
          DocTransformer savedTransformer = savedReturnFields.getTransformer();
          Query filterQuery = docComponent.getFilterQuery(roles);
          if (filterQuery != null) {
            SolrReturnFields solrReturnFields = new AddDocIdReturnFields(rb.req, savedTransformer, filterQuery);
            rb.rsp.setReturnFields(solrReturnFields);
          } else {
            throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED,
              "Request from user: " + userName +
              "rejected because filter query was unable to be generated");
          }
        } else {
          throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED,
            "Request from user: " + userName +
            " rejected because user is not associated with any roles");
        }
      }
    } else {
      throw new SolrException(SolrException.ErrorCode.UNAUTHORIZED,
        "RealTimeGetRequest request " +
        " rejected because \"queryDocAuthorization\" component not defined");
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (!(rb.rsp.getReturnFields() instanceof AddDocIdReturnFields)) {
      log.info("Skipping application of SecureRealTimeGetComponent because "
          + " return field wasn't applied in prepare phase");
      return;
    }

    final SolrQueryResponse rsp = rb.rsp;
    ResponseFormatDocs responseFormatDocs = getResponseFormatDocs(rsp);
    if (responseFormatDocs == null) {
      return; // no documents to check
    }
    final SolrDocumentList docList = responseFormatDocs.getDocList();
    final AddDocIdReturnFields addDocIdRf = (AddDocIdReturnFields)rb.rsp.getReturnFields();
    final Query filterQuery = addDocIdRf.getFilterQuery();
    final DocTransformer transformer = addDocIdRf.getOriginalTransformer();

    // we replaced the original transfer in order to add the document id, reapply it here
    // so return documents in the correct format.
    if (transformer != null) {
      TransformContext context = new TransformContext();
      context.req = rb.req;
      transformer.setContext(context);
    }

    SolrCore core = rb.req.getCore();
    UpdateLog ulog = core.getUpdateHandler().getUpdateLog();
    SchemaField idField = core.getLatestSchema().getUniqueKeyField();
    FieldType fieldType = idField.getType();
    boolean openedRealTimeSearcher = false;
    RefCounted<SolrIndexSearcher> searcherHolder = core.getRealtimeSearcher();

    SolrDocumentList docListToReturn = new SolrDocumentList();
    try {
      SolrIndexSearcher searcher = searcherHolder.get();
      for (SolrDocument doc : docList) {
        // -1 doc id indicates this value was read from log; we need to open
        // a new real time searcher to run the filter query against
        if (Integer.valueOf(-1).equals(doc.get(ID_FIELD_NAME)) && !openedRealTimeSearcher) {
          searcherHolder.decref();
          // hack to clear ulog maps since we don't have
          // openRealtimeSearcher API from SOLR-8436
          AddUpdateCommand cmd = new AddUpdateCommand(rb.req);
          cmd.setFlags(UpdateCommand.REPLAY);
          ulog.add(cmd, true);

          searcherHolder = core.getRealtimeSearcher();
          searcher = searcherHolder.get();
          openedRealTimeSearcher = true;
        }

        int docid = getFilteredInternalDocId(doc, idField, fieldType, filterQuery, searcher);
        if (docid < 0) continue;
        Document luceneDocument = searcher.doc(docid);
        SolrDocument newDoc = toSolrDoc(luceneDocument,  core.getLatestSchema());
        if( transformer != null ) {
          transformer.transform(newDoc, docid);
        }
        docListToReturn.add(newDoc);
      }
    } finally {
      searcherHolder.decref();
      searcherHolder = null;
    }
    if (responseFormatDocs.getUseResponseField()) {
      rsp.getValues().remove("response");
      docListToReturn.setNumFound(docListToReturn.size());
      rsp.add("response", docListToReturn);
    } else {
      rsp.getValues().remove("doc");
      rsp.add("doc", docListToReturn.size() > 0 ? docListToReturn.get(0) : null);
    }
  }

  private static SolrDocument toSolrDoc(Document doc, IndexSchema schema) {
    SolrDocument out = new SolrDocument();
    for ( IndexableField f : doc.getFields() ) {
      // Make sure multivalued fields are represented as lists
      Object existing = out.get(f.name());
      if (existing == null) {
        SchemaField sf = schema.getFieldOrNull(f.name());

        // don't return copyField targets
        if (sf != null && schema.isCopyFieldTarget(sf)) continue;

        if (sf != null && sf.multiValued()) {
          List<Object> vals = new ArrayList<>();
          vals.add( f );
          out.setField( f.name(), vals );
        }
        else{
          out.setField( f.name(), f );
        }
      }
      else {
        out.addField( f.name(), f );
      }
    }
    return out;
  }

  // get the response format to use and the documents to check
  private static ResponseFormatDocs getResponseFormatDocs(final SolrQueryResponse rsp) {
    SolrDocumentList docList = (SolrDocumentList)rsp.getValues().get("response");
    SolrDocument singleDoc = (SolrDocument)rsp.getValues().get("doc");
    if (docList == null && singleDoc == null) {
      return null; // no documents to filter
    }
    if (docList != null && singleDoc != null) {
       throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Not able to filter secure reponse, RealTimeGet returned both a doc list and " +
          "an individual document");
    }
    final boolean useResponseField = docList != null;
    if (docList == null) {
      docList = new SolrDocumentList();
      docList.add(singleDoc);
    }
    return new ResponseFormatDocs(useResponseField, docList);
  }

  /**
   * @param doc SolrDocument to check
   * @param idField field where the id is stored
   * @param fieldType type of id field
   * @param filterQuery Query to filter by
   * @param searcher SolrIndexSearcher on which to apply the filter query
   * @returns the internal docid, or -1 if doc is not found or doesn't match filter
   */
  private static int getFilteredInternalDocId(SolrDocument doc, SchemaField idField, FieldType fieldType,
        Query filterQuery, SolrIndexSearcher searcher) throws IOException {
    int docid = -1;
    Field f = (Field)doc.getFieldValue(idField.getName());
    String idStr = f.stringValue();
    BytesRef idBytes = new BytesRef();
    fieldType.readableToIndexed(idStr, idBytes);
    // get the internal document id
    long segAndId = searcher.lookupId(idBytes);

      // if docid is valid, run it through the filter
    if (segAndId >= 0) {
      int segid = (int) segAndId;
      AtomicReaderContext ctx = searcher.getTopReaderContext().leaves().get((int) (segAndId >> 32));
      docid = segid + ctx.docBase;
      Weight weight = filterQuery.createWeight(searcher);
      Scorer scorer = weight.scorer(ctx, null);
      if (scorer == null || segid != scorer.advance(segid)) {
        // filter doesn't match.
        docid = -1;
      }
    }
    return docid;
  }

  @Override
  public String getDescription() {
    return "Handle Query Document Authorization for RealTimeGet";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

  private static class ResponseFormatDocs {
    private boolean useResponseField;
    private SolrDocumentList docList;

    public ResponseFormatDocs(boolean useResponseField, SolrDocumentList docList) {
      this.useResponseField = useResponseField;
      this.docList = docList;
    }

    public boolean getUseResponseField() { return useResponseField; }
    public SolrDocumentList getDocList() { return docList; }
  }

  // ReturnField that adds a transformer to store the document id
  private static class AddDocIdReturnFields extends SolrReturnFields {
    private DocTransformer transformer;
    private DocTransformer originalTransformer;
    private Query filterQuery;

    public AddDocIdReturnFields(SolrQueryRequest req, DocTransformer docTransformer,
        Query filterQuery) {
      super(req);
      this.originalTransformer = docTransformer;
      this.filterQuery = filterQuery;
      final DocTransformers docTransformers = new DocTransformers();
      if (originalTransformer != null) docTransformers.addTransformer(originalTransformer);
      docTransformers.addTransformer(new DocIdAugmenter(ID_FIELD_NAME));
      this.transformer = docTransformers;
    }

    @Override
    public DocTransformer getTransformer() {
      return transformer;
    }

    public DocTransformer getOriginalTransformer() {
      return originalTransformer;
    }

    public Query getFilterQuery() {
      return filterQuery;
    }
  }

  // the Solr DocIdAugmenterFactory does not store negative doc ids;
  // we do here.
  private static class DocIdAugmenter extends DocTransformer
  {
    final String name;

    public DocIdAugmenter( String display )
    {
      this.name = display;
    }

    @Override
    public String getName()
    {
      return name;
    }

    @Override
    public void transform(SolrDocument doc, int docid) {
      doc.setField( name, docid );
    }
  }

}
