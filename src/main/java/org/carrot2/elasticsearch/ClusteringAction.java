package org.carrot2.elasticsearch;

import static org.carrot2.elasticsearch.LoggerUtils.emitErrorResponse;
import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.carrot2.core.Cluster;
import org.carrot2.core.Controller;
import org.carrot2.core.Document;
import org.carrot2.core.LanguageCode;
import org.carrot2.core.ProcessingException;
import org.carrot2.core.ProcessingResult;
import org.carrot2.core.attribute.CommonAttributesDescriptor;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.common.base.Preconditions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import com.google.common.collect.Maps;

/**
 * Perform clustering of search results.
 */
public class ClusteringAction 
    extends Action<ClusteringAction.ClusteringActionRequest, 
                   ClusteringAction.ClusteringActionResponse, 
                   ClusteringAction.ClusteringActionRequestBuilder> {
    /* Action name. */
    public static final String NAME = "clustering/cluster";

    /* Reusable singleton. */
    public static final ClusteringAction INSTANCE = new ClusteringAction();

    private ClusteringAction() {
        super(NAME);
    }

    @Override
    public ClusteringActionRequestBuilder newRequestBuilder(Client client) {
        return new ClusteringActionRequestBuilder(client);
    }

    @Override
    public ClusteringActionResponse newResponse() {
        return new ClusteringActionResponse();
    }
    
    /**
     * An {@link ActionRequest} for {@link ClusteringAction}. 
     */
    public static class ClusteringActionRequest extends ActionRequest<ClusteringActionRequest> {
        private SearchRequest searchRequest;
        private String queryHint;
        private List<FieldMappingSpec> fieldMapping = Lists.newArrayList();
        private String algorithm;
        private Map<String, Object> attributes;

        /**
         * Set the {@link SearchRequest} to use for fetching documents to be clustered.
         * The search request must fetch enough documents for clustering to make sense
         * (set <code>size</code> appropriately).
         */
        public ClusteringActionRequest setSearchRequest(SearchRequest searchRequest) {
            this.searchRequest = searchRequest;
            return this;
        }

        /**
         * @see #setSearchRequest(SearchRequest)
         */
        public ClusteringActionRequest setSearchRequest(SearchRequestBuilder builder) {
            return setSearchRequest(builder.request());
        }

        public SearchRequest getSearchRequest() {
            return searchRequest;
        }

        /**
         * @param queryHint A set of terms which correspond to the query. This hint helps the
         *  clustering algorithm to avoid trivial clusters around the query terms. Typically the query
         *  terms hint will be identical to what the user typed in the search box.
         *  <p>
         *  The hint may be an empty string but must not be <code>null</code>.
         */
        public ClusteringActionRequest setQueryHint(String queryHint) {
            this.queryHint = queryHint;
            return this;
        }
        
        /**
         * @see #setQueryHint(String)
         */
        public String getQueryHint() {
            return queryHint;
        }

        /**
         * Sets the identifier of the clustering algorithm to use. If <code>null</code>, the default
         * algorithm will be used (depending on what's available).
         */
        public ClusteringActionRequest setAlgorithm(String algorithm) {
            this.algorithm = algorithm;
            return this;
        }
        
        /**
         * @see #setAlgorithm
         */
        public String getAlgorithm() {
            return algorithm;
        }

        /**
         * Sets a map of runtime override attributes for clustering algorithms. 
         */
        public ClusteringActionRequest setAttributes(Map<String, Object> map) {
            this.attributes = map;
            return this;
        }
        
        /**
         * @see #setAttributes(Map)
         */
        public Map<String, Object> getAttributes() {
            return attributes;
        }

        /**
         * Map a hit's field to a logical section of a document to be clustered (title, content or URL).
         * @see LogicalField
         */
        public ClusteringActionRequest addFieldMapping(String fieldName, LogicalField logicalField) {
            fieldMapping.add(new FieldMappingSpec(fieldName, logicalField, FieldSource.FIELD));
            return this;
        }

        /**
         * Map a hit's source field (field unpacked from the <code>_source</code> document)
         * to a logical section of a document to be clustered (title, content or URL).
         * @see LogicalField
         */
        public ClusteringActionRequest addSourceFieldMapping(String sourceFieldName, LogicalField logicalField) {
            fieldMapping.add(new FieldMappingSpec(sourceFieldName, logicalField, FieldSource.SOURCE));
            return this;
        }

        /**
         * Map a hit's highligted field (fragments of the original field) to a logical section
         * of a document to be clustered. This may be used to decrease the amount of information
         * passed to the clustering engine but also to "focus" the clustering engine on the context
         * of the query.
         */
        public ClusteringActionRequest addHighlightedFieldMapping(String fieldName, LogicalField logicalField) {
            fieldMapping.add(new FieldMappingSpec(fieldName, logicalField, FieldSource.HIGHLIGHT));
            return this;
        }

        /**
         * Add a (valid!) field mapping specification to a logical field.
         * @see FieldSource
         */
        public ClusteringActionRequest addFieldMappingSpec(String fieldSpec, LogicalField logicalField) {
            FieldSource.ParsedFieldSource pfs = FieldSource.parseSpec(fieldSpec);
            if (pfs.source != null) {
                switch (pfs.source) {
                    case HIGHLIGHT:
                        addHighlightedFieldMapping(pfs.fieldName, logicalField);
                        break;

                    case FIELD:
                        addFieldMapping(pfs.fieldName, logicalField);
                        break;

                    case SOURCE:
                        addSourceFieldMapping(pfs.fieldName, logicalField);
                        break;

                    default:
                        throw new RuntimeException();
                }
            }

            if (pfs.source == null) {
                throw new ElasticSearchException("Field mapping specification must contain a " +
                        " valid source prefix for the field source: " + fieldSpec);
            }
            
            return this;
        }

        /** Access to prepared field mapping. */
        List<FieldMappingSpec> getFieldMapping() {
            return fieldMapping;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (searchRequest == null) {
                validationException = addValidationError("No delegate search request", validationException);
            }
            
            if (queryHint == null) {
                validationException = addValidationError("query hint may be empty but must not be null.", validationException);
            }
            
            if (fieldMapping.isEmpty()) {
                validationException = addValidationError("At least one field should be mapped to a logical document field.", validationException);
            }

            ActionRequestValidationException ex = searchRequest.validate();
            if (ex != null) {
                if (validationException == null) {
                    validationException = new ActionRequestValidationException();
                }
                validationException.addValidationErrors(ex.validationErrors());
            }
            
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert searchRequest != null;
            this.searchRequest.writeTo(out);
            out.writeOptionalString(queryHint);
            out.writeOptionalString(algorithm);

            out.writeVInt(fieldMapping.size());
            for (FieldMappingSpec spec : fieldMapping) {
                spec.writeTo(out);
            }
            
            boolean hasAttributes = (attributes != null);
            out.writeBoolean(hasAttributes);
            if (hasAttributes) {
                out.writeMap(attributes);
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.readFrom(in);
            
            this.searchRequest = searchRequest;
            this.queryHint = in.readOptionalString();
            this.algorithm = in.readOptionalString();
            
            int count = in.readVInt();
            while (count-- > 0) {
                FieldMappingSpec spec = new FieldMappingSpec();
                spec.readFrom(in);
                fieldMapping.add(spec);
            }
            
            boolean hasAttributes = in.readBoolean();
            if (hasAttributes) {
                attributes = in.readMap();
            }
        }
    }    

    /**
     * An {@link ActionRequestBuilder} for {@link ClusteringAction}.
     */
    public static class ClusteringActionRequestBuilder 
        extends ActionRequestBuilder<ClusteringActionRequest, 
                                     ClusteringActionResponse, 
                                     ClusteringActionRequestBuilder>
    {

        public ClusteringActionRequestBuilder(Client client) {
            super((InternalClient) client, new ClusteringActionRequest());
        }
    
        public ClusteringActionRequestBuilder setSearchRequest(SearchRequestBuilder builder) {
            super.request.setSearchRequest(builder);
            return this;
        }
    
        public ClusteringActionRequestBuilder setSearchRequest(SearchRequest searchRequest) {
            super.request.setSearchRequest(searchRequest);
            return this;
        }
        
        public ClusteringActionRequestBuilder setQueryHint(String queryHint) {
            if (queryHint == null) {
                throw new IllegalArgumentException("Query hint may be empty but must not be null.");
            }
            super.request.setQueryHint(queryHint);
            return this;
        }
    
        public ClusteringActionRequestBuilder setAlgorithm(String algorithm) {
            super.request.setAlgorithm(algorithm);
            return this;
        }
    
        public ClusteringActionRequestBuilder addAttributes(Map<String,Object> attributes) {
            if (super.request.getAttributes() == null) {
                super.request.setAttributes(Maps.<String,Object> newHashMap());
            }
            super.request.getAttributes().putAll(attributes);
            return this;
        }
    
        public ClusteringActionRequestBuilder addAttribute(String key, Object value) {
            return addAttributes(ImmutableMap.of(key, value));
        }
    
        public ClusteringActionRequestBuilder setAttributes(Map<String,Object> attributes) {
            super.request.setAttributes(attributes);
            return this;
        }
    
        public ClusteringActionRequestBuilder addFieldMapping(String fieldName, LogicalField logicalField) {
            super.request.addFieldMapping(fieldName, logicalField);
            return this;
        }
    
        public ClusteringActionRequestBuilder addSourceFieldMapping(String fieldName, LogicalField logicalField) {
            super.request.addSourceFieldMapping(fieldName, logicalField);
            return this;
        }
    
        public ClusteringActionRequestBuilder addHighlightedFieldMapping(String fieldName, LogicalField logicalField) {
            super.request.addHighlightedFieldMapping(fieldName, logicalField);
            return this;
        }
    
        public ClusteringActionRequestBuilder addFieldMappingSpec(String fieldSpec, LogicalField logicalField) {
            super.request.addFieldMappingSpec(fieldSpec, logicalField);
            return this;
        }
    
        @Override
        protected void doExecute(
                ActionListener<ClusteringActionResponse> listener) {
            ((Client) client).execute(ClusteringAction.INSTANCE, request, listener);
        }
    }

    /**
     * An {@link ActionResponse} for {@link ClusteringAction}. 
     */
    public static class ClusteringActionResponse extends ActionResponse implements ToXContent {
        /**
         * Clustering-related response fields. 
         */
        static final class Fields {
            static final XContentBuilderString SEARCH_RESPONSE = new XContentBuilderString("search_response");
            static final XContentBuilderString CLUSTERS = new XContentBuilderString("clusters");
            static final XContentBuilderString INFO = new XContentBuilderString("info");
            
            /**
             * {@link Fields#INFO} keys.
             */
            static final class Info {
                public static final String ALGORITHM = "algorithm";
                public static final String SEARCH_MILLIS = "search-millis";
                public static final String CLUSTERING_MILLIS = "clustering-millis";
                public static final String TOTAL_MILLIS = "total-millis";
            }
        }

        private SearchResponse searchResponse;
        private DocumentGroup [] topGroups;
        private Map<String,String> info;

        ClusteringActionResponse() {
        }

        public ClusteringActionResponse(
                SearchResponse searchResponse, 
                DocumentGroup[] topGroups,
                Map<String,String> info) {
            this.searchResponse = Preconditions.checkNotNull(searchResponse);
            this.topGroups = Preconditions.checkNotNull(topGroups);
            this.info = Collections.unmodifiableMap(Preconditions.checkNotNull(info));
        }

        public SearchResponse getSearchResponse() {
            return searchResponse;
        }

        public DocumentGroup[] getDocumentGroups() {
            return topGroups;
        }
        
        public Map<String, String> getInfo() {
            return info;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params)
                throws IOException {
            if (searchResponse != null) {
                searchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
            }

            builder.startArray(Fields.CLUSTERS);
            if (topGroups != null) {
                for (DocumentGroup group : topGroups) {
                    group.toXContent(builder, params);
                }
            }
            builder.endArray();
            builder.field(Fields.INFO, info);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            
            boolean hasSearchResponse = searchResponse != null;
            out.writeBoolean(hasSearchResponse);
            if (hasSearchResponse) {
                this.searchResponse.writeTo(out);
            }

            out.writeVInt(topGroups == null ? 0 : topGroups.length);
            if (topGroups != null) {
                for (DocumentGroup group : topGroups) {
                    group.writeTo(out);
                }
            }
            
            out.writeVInt(info == null ? 0 : info.size());
            if (info != null) {
                for (Map.Entry<String,String> e : info.entrySet()) {
                    out.writeOptionalString(e.getKey());
                    out.writeOptionalString(e.getValue());
                }
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);

            boolean hasSearchResponse = in.readBoolean();
            if (hasSearchResponse) {
                this.searchResponse = new SearchResponse();
                this.searchResponse.readFrom(in);
            }

            int documentGroupsCount = in.readVInt();
            topGroups = new DocumentGroup [documentGroupsCount];
            for (int i = 0; i < documentGroupsCount; i++) {
                DocumentGroup group = new DocumentGroup();
                group.readFrom(in);
                topGroups[i] = group;
            }
            
            int entries = in.readVInt();
            info = Maps.newLinkedHashMap();
            for (int i = 0; i < entries; i++) {
                info.put(in.readOptionalString(), in.readOptionalString());
            }
        }

        @Override
        public String toString() {
            return ToString.objectToJson(this);
        }
    }
    
    /**
     * A {@link TransportAction} for {@link ClusteringAction}.
     */
    public static class TransportClusteringAction  
        extends TransportAction<ClusteringAction.ClusteringActionRequest,
                                ClusteringAction.ClusteringActionResponse>
    {
        private final Set<String> langCodeWarnings = Sets.newCopyOnWriteArraySet();
    
        private final TransportSearchAction searchAction;
        private final ControllerSingleton controllerSingleton;
    
        @Inject
        protected TransportClusteringAction(Settings settings, ThreadPool threadPool,
                TransportService transportService,
                TransportSearchAction searchAction,
                ControllerSingleton controllerSingleton) {
            super(settings, threadPool);
            this.searchAction = searchAction;
            this.controllerSingleton = controllerSingleton;
            transportService.registerHandler(ClusteringAction.NAME, new TransportHandler());
        }
    
        @Override
        protected void doExecute(final ClusteringActionRequest clusteringRequest,
                                 final ActionListener<ClusteringActionResponse> listener) {
            final long tsSearchStart = System.nanoTime();
            searchAction.execute(clusteringRequest.getSearchRequest(), new ActionListener<SearchResponse>() {
                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
    
                @Override
                public void onResponse(SearchResponse response) {
                    final long tsSearchEnd = System.nanoTime();
    
                    List<String> algorithmComponentIds = controllerSingleton.getAlgorithms();
                    String algorithmId = clusteringRequest.getAlgorithm();
                    if (algorithmId == null) {
                        algorithmId = algorithmComponentIds.get(0);                    
                    } else {
                        if (!algorithmComponentIds.contains(algorithmId)) {
                            listener.onFailure(new ElasticSearchException("No such algorithm: " + algorithmId));
                            return;
                        }
                    }
                    
                    /*
                     * We're not a threaded listener so we're running on the search thread. This
                     * is good -- we don't want to serve more clustering requests than we can handle
                     * anyway. 
                     */
                    Controller controller = controllerSingleton.getController();
    
                    Map<String, Object> processingAttrs = Maps.newHashMap();
                    Map<String, Object> requestAttrs = clusteringRequest.getAttributes();
                    if (requestAttrs != null) {
                        processingAttrs.putAll(requestAttrs);
                    }

                    try {
                        CommonAttributesDescriptor.attributeBuilder(processingAttrs)
                            .documents(prepareDocumentsForClustering(clusteringRequest, response))
                            .query(clusteringRequest.getQueryHint());
        
                        final long tsClusteringStart = System.nanoTime();
                        final ProcessingResult result = controller.process(processingAttrs, algorithmId);
                        final DocumentGroup[] groups = adapt(result.getClusters());
                        final long tsClusteringEnd = System.nanoTime();
        
                        final Map<String,String> info = ImmutableMap.of(
                            ClusteringActionResponse.Fields.Info.ALGORITHM, algorithmId,
                            ClusteringActionResponse.Fields.Info.SEARCH_MILLIS, Long.toString(TimeUnit.NANOSECONDS.toMillis(tsSearchEnd - tsSearchStart)),
                            ClusteringActionResponse.Fields.Info.CLUSTERING_MILLIS, Long.toString(TimeUnit.NANOSECONDS.toMillis(tsClusteringEnd - tsClusteringStart)),
                            ClusteringActionResponse.Fields.Info.TOTAL_MILLIS, Long.toString(TimeUnit.NANOSECONDS.toMillis(tsClusteringEnd - tsSearchStart)));
        
                        listener.onResponse(new ClusteringActionResponse(response, groups, info));
                    } catch (ProcessingException e) {
                        // Log a full stack trace with all nested exceptions but only return 
                        // ElasticSearchException exception with a simple String (otherwise 
                        // clients cannot deserialize exception classes).
                        String message = "Search results clustering error: " + e.getMessage();
                        logger.warn(message, e);
                        listener.onFailure(new ElasticSearchException(message));
                        return;
                    }
                }
            });
        }
    
        /* */
        protected DocumentGroup[] adapt(List<Cluster> clusters) {
            DocumentGroup [] groups = new DocumentGroup [clusters.size()];
            for (int i = 0; i < groups.length; i++) {
                groups[i] = adapt(clusters.get(i));
            }
            return groups;
        }
    
        /* */
        private DocumentGroup adapt(Cluster cluster) {
            DocumentGroup group = new DocumentGroup();
            group.setId(cluster.getId());
            List<String> phrases = cluster.getPhrases();
            group.setPhrases(phrases.toArray(new String[phrases.size()]));
            group.setLabel(cluster.getLabel());
            group.setScore(cluster.getScore());
            group.setOtherTopics(cluster.isOtherTopics());
    
            List<Document> documents = cluster.getDocuments();
            String[] documentReferences = new String[documents.size()];
            for (int i = 0; i < documentReferences.length; i++) {
                documentReferences[i] = documents.get(i).getStringId();
            }
            group.setDocumentReferences(documentReferences);
    
            List<Cluster> subclusters = cluster.getSubclusters();
            subclusters = (subclusters == null ? Collections.<Cluster> emptyList() : subclusters);
            group.setSubgroups(adapt(subclusters));
    
            return group;
        }
    
        /**
         * Map {@link SearchHit} fields to logical fields of Carrot2 {@link Document}.
         */
        private List<Document> prepareDocumentsForClustering(
                final ClusteringActionRequest request,
                SearchResponse response) {
            SearchHit [] hits = response.getHits().hits();
            List<Document> documents = Lists.newArrayListWithCapacity(hits.length);
            List<FieldMappingSpec> fieldMapping = request.getFieldMapping();
            StringBuilder title = new StringBuilder();
            StringBuilder content = new StringBuilder();
            StringBuilder url = new StringBuilder();
            StringBuilder language = new StringBuilder();
            Joiner joiner = Joiner.on(" . ");
            
            boolean emptySourceWarningEmitted = false;
    
            for (SearchHit hit : hits) {
                // Prepare logical fields for each hit.
                title.setLength(0);
                content.setLength(0);
                url.setLength(0);
                language.setLength(0);
                
                Map<String, SearchHitField> fields = hit.getFields();
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
    
                Map<String,Object> sourceAsMap = null;
                for (FieldMappingSpec spec : fieldMapping) {
                    // Determine the content source.
                    Object appendContent = null;
                    switch (spec.source) {
                        case FIELD:
                            SearchHitField searchHitField = fields.get(spec.field);
                            if (searchHitField != null) {
                                appendContent = searchHitField.getValue();
                            }
                            break;
                        
                        case HIGHLIGHT:
                            HighlightField highlightField = highlightFields.get(spec.field);
                            if (highlightField != null) {
                                appendContent = joiner.join(highlightField.fragments());
                            }
                            break;
    
                        case SOURCE:
                            if (sourceAsMap == null) {
                                if (hit.isSourceEmpty()) {
                                    if (!emptySourceWarningEmitted) {
                                        emptySourceWarningEmitted = true;
                                        logger.warn("_source field mapping used but no source available for: {}, field {}", hit.getId(), spec.field);
                                    }
                                } else {
                                    sourceAsMap = hit.getSource();
                                }
                            }
                            if (sourceAsMap != null) {
                                appendContent = sourceAsMap.get(spec.field);
                            }
                            break;
    
                        default:
                            throw org.carrot2.elasticsearch.Preconditions.unreachable();
                    }
    
                    // Determine the target field.
                    if (appendContent != null) {
                        StringBuilder target;
                        switch (spec.logicalField) {
                            case URL:
                                url.setLength(0); // Clear previous (single mapping allowed).
                                target = url;
                                break;
                            case LANGUAGE:
                                language.setLength(0); // Clear previous (single mapping allowed);
                                target = language;
                                break;
                            case TITLE:
                                target = title;
                                break;
                            case CONTENT:
                                target = content;
                                break;
                            default:
                                throw org.carrot2.elasticsearch.Preconditions.unreachable();
                        }
    
                        // Separate multiple fields with a single dot (prevent accidental phrase gluing).
                        if (appendContent != null) {
                            if (target.length() > 0) {
                                target.append(" . ");
                            }
                            target.append(appendContent);
                        }
                    }
                }
    
                LanguageCode langCode = null;
                if (language.length() > 0) {
                    String langCodeString = language.toString();
                    langCode = LanguageCode.forISOCode(langCodeString);
                    if (langCode == null && langCodeWarnings.add(langCodeString)) {
                        logger.warn("Language mapping not a supported ISO639-1 code: {}", langCodeString);
                    }
                }
    
                Document doc = new Document(
                        title.toString(),
                        content.toString(),
                        url.toString(),
                        langCode,
                        hit.id());
    
                documents.add(doc);
            }
    
            return documents;
        }
    
        private final class TransportHandler extends BaseTransportRequestHandler<ClusteringActionRequest> {
            @Override
            public ClusteringActionRequest newInstance() {
                return new ClusteringActionRequest();
            }
    
            @Override
            public void messageReceived(final ClusteringActionRequest request, final TransportChannel channel) throws Exception {
                request.listenerThreaded(false);
                execute(request, new ActionListener<ClusteringActionResponse>() {
                    @Override
                    public void onResponse(ClusteringActionResponse response) {
                        try {
                            channel.sendResponse(response);
                        } catch (Exception e) {
                            onFailure(e);
                        }
                    }
    
                    @Override
                    public void onFailure(Throwable e) {
                        try {
                            channel.sendResponse(e);
                        } catch (Exception e1) {
                            logger.warn("Failed to send error response for action ["
                                    + ClusteringAction.NAME + "] and request [" + request + "]", e1);
                        }
                    }
                });
            }
    
            @Override
            public String executor() {
                return ThreadPool.Names.SAME;
            }
        }
    }

    /**
     * An {@link BaseRestHandler} for {@link ClusteringAction}.
     */
    public static class RestClusteringAction extends BaseRestHandler {
        /**
         * Action name suffix.
         */
        public static String NAME = "_search_with_clusters";

        @Inject
        public RestClusteringAction(
                Settings settings, 
                Client client, 
                RestController controller) {
            super(settings, client);

            controller.registerHandler(POST, "/" + NAME,                this);
            controller.registerHandler(POST, "/{index}/" + NAME,        this);
            controller.registerHandler(POST, "/{index}/{type}/" + NAME, this);
        }

        @Override
        public void handleRequest(final RestRequest request, final RestChannel channel) {
            if (!request.hasContent()) {
                emitErrorResponse(channel, request, logger, new RuntimeException("Request body was expected."));
                return;
            }

            // Fill action request with data from REST request.
            // TODO: delegate json parsing to Carrot2ClusteringAction.
            ClusteringActionRequest actionRequest = new ClusteringActionRequest();
            fillFromSource(request, actionRequest, request.content());

            // Build a clustering request and dispatch.
            client.execute(ClusteringAction.INSTANCE, actionRequest, 
                new ActionListener<ClusteringActionResponse>() {
                @Override
                public void onResponse(ClusteringActionResponse response) {
                    try {
                        XContentBuilder builder = restContentBuilder(request);
                        builder.startObject();
                        response.toXContent(builder, request);
                        builder.endObject();
                        channel.sendResponse(
                                new XContentRestResponse(
                                        request, 
                                        response.getSearchResponse().status(), 
                                        builder));
                    } catch (Exception e) {
                        logger.debug("Failed to emit response.", e);
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    emitErrorResponse(channel, request, logger, e);
                }
            });
        }

        /**
         * Parse the clustering/ search request JSON.
         */
        @SuppressWarnings("unchecked")
        private void fillFromSource(
                RestRequest restRequest, 
                ClusteringActionRequest clusteringRequest,
                BytesReference source) {
            if (source == null || source.length() == 0) {
                return;
            }

            XContentParser parser = null;
            try {
                // TODO: we should avoid reparsing here but it's terribly difficult to slice
                // the underlying byte buffer to get just the search request.

                parser = XContentFactory.xContent(source).createParser(source);
                Map<String, Object> asMap = parser.mapOrderedAndClose();

                if (asMap.get("query_hint") != null) {
                    clusteringRequest.setQueryHint((String) asMap.get("query_hint"));
                }
                if (asMap.containsKey("field_mapping")) {
                    parseFieldSpecs(clusteringRequest, (Map<String,List<String>>) asMap.get("field_mapping"));
                }
                if (asMap.containsKey("search_request")) {
                    SearchRequest searchRequest = new SearchRequest();
                    searchRequest.indices(Strings.splitStringByCommaToArray(restRequest.param("index")));
                    searchRequest.source((Map<?,?>) asMap.get("search_request"));
                    searchRequest.types(Strings.splitStringByCommaToArray(restRequest.param("type")));
                    clusteringRequest.setSearchRequest(searchRequest);
                }
                if (asMap.containsKey("algorithm")) {
                    clusteringRequest.setAlgorithm((String) asMap.get("algorithm"));
                }
                if (asMap.containsKey("attributes")) {
                    clusteringRequest.setAttributes((Map<String,Object>) asMap.get("attributes"));
                }
            } catch (Exception e) {
                String sSource = "_na_";
                try {
                    sSource = XContentHelper.convertToJson(source, false);
                } catch (Throwable e1) {
                    // ignore
                }
                throw new ElasticSearchException("Failed to parse source [" + sSource + "]", e);
            } finally {
                if (parser != null) {
                    parser.close();
                }
            }        
        }

        private void parseFieldSpecs(ClusteringActionRequest actionRequest,
                Map<String, List<String>> fieldSpecs) {
            for (Map.Entry<String,List<String>> e : fieldSpecs.entrySet()) {
                LogicalField logicalField = LogicalField.valueOfCaseInsensitive(e.getKey());
                if (logicalField != null) {
                    for (String fieldSpec : e.getValue()) {
                        actionRequest.addFieldMappingSpec(fieldSpec, logicalField);
                    }
                }
            }
        }
    }    
}
