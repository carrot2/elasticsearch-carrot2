package org.carrot2.elasticsearch;

import org.carrot2.core.Cluster;
import org.carrot2.core.Controller;
import org.carrot2.core.Document;
import org.carrot2.core.LanguageCode;
import org.carrot2.core.ProcessingException;
import org.carrot2.core.ProcessingResult;
import org.carrot2.core.attribute.CommonAttributesDescriptor;
import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchRequestParsers;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import static org.carrot2.elasticsearch.LoggerUtils.emitErrorResponse;
import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

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
    public ClusteringActionResponse newResponse() {
        return new ClusteringActionResponse();
    }

    @Override
    public ClusteringActionRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new ClusteringActionRequestBuilder(client);
    }

    /**
     * An {@link ActionRequest} for {@link ClusteringAction}.
     */
    public static class ClusteringActionRequest extends ActionRequest {
        private SearchRequest searchRequest;
        private String queryHint;
        private List<FieldMappingSpec> fieldMapping = new ArrayList<>();
        private String algorithm;
        private int maxHits = Integer.MAX_VALUE;
        private Map<String, Object> attributes;

        /**
         * Set the {@link SearchRequest} to use for fetching documents to be clustered.
         * The search request must fetch enough documents for clustering to make sense
         * (set <code>size</code> appropriately).
         * @param searchRequest search request to set
         * @return same builder instance
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
         *                  clustering algorithm to avoid trivial clusters around the query terms. Typically the query
         *                  terms hint will be identical to what the user typed in the search box.
         *                  <p>
         *                  The hint may be an empty string but must not be <code>null</code>.
         * @return same builder instance
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
         * @see #getIncludeHits
         * @deprecated Use {@link #setMaxHits} and set it to zero instead.
         */
        @Deprecated()
        public boolean getIncludeHits() {
            return maxHits > 0;
        }

        /**
         * Sets whether to include hits with clustering results. If only cluster labels
         * are needed the hits may be omitted to save bandwidth.
         *
         * @deprecated Use {@link #setMaxHits} instead.
         */
        @Deprecated()
        public ClusteringActionRequest setIncludeHits(boolean includeHits) {
            if (includeHits) {
                setMaxHits(Integer.MAX_VALUE);
            } else {
                setMaxHits(0);
            }
            return this;
        }

        /**
         * Sets the maximum number of hits to return with the response. Setting this
         * value to zero will only return clusters, without any hits (can be used
         * to save bandwidth if only cluster labels are needed).
         * <p>
         * Set to {@link Integer#MAX_VALUE} to include all the hits.
         */
        public void setMaxHits(int maxHits) {
            assert maxHits >= 0;
            this.maxHits = maxHits;
        }

        /**
         * Sets {@link #setMaxHits(int)} from a string. An empty string or null means
         * all hits should be included.
         */
        public void setMaxHits(String value) {
            if (value == null || value.trim().isEmpty()) {
                setMaxHits(Integer.MAX_VALUE);
            } else {
                setMaxHits(Integer.parseInt(value));
            }
        }

        /**
         * Returns the maximum number of hits to be returned as part of the response.
         * If equal to {@link Integer#MAX_VALUE}, then all hits will be returned.
         */
        public int getMaxHits() {
            return maxHits;
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
         * Parses some {@link org.elasticsearch.common.xcontent.XContent} and fills in the request.
         */
        @SuppressWarnings("unchecked")
        public void source(BytesReference source, SearchRequestParsers searchRequestParsers) {
            if (source == null || source.length() == 0) {
                return;
            }

            try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
                // TODO: we should avoid reparsing search_request here 
                // but it's terribly difficult to slice the underlying byte 
                // buffer to get just the search request.
                Map<String, Object> asMap = parser.mapOrdered();

                String queryHint = (String) asMap.get("query_hint");
                if (queryHint != null) {
                    setQueryHint(queryHint);
                }

                Map<String, List<String>> fieldMapping = (Map<String, List<String>>) asMap.get("field_mapping");
                if (fieldMapping != null) {
                    parseFieldSpecs(fieldMapping);
                }

                String algorithm = (String) asMap.get("algorithm");
                if (algorithm != null) {
                    setAlgorithm(algorithm);
                }

                Map<String, Object> attributes = (Map<String, Object>) asMap.get("attributes");
                if (attributes != null) {
                    setAttributes(attributes);
                }

                Map<String, ?> searchRequestMap = (Map<String, ?>) asMap.get("search_request");
                if (searchRequestMap != null) {
                    if (this.searchRequest == null) {
                        searchRequest = new SearchRequest();
                    }

                    XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).map(searchRequestMap);
                    XContentParser searchXParser = XContentFactory.xContent(XContentType.JSON).createParser(builder.bytes());
                    QueryParseContext parseContext = new QueryParseContext(searchRequestParsers.queryParsers,
                                                                           searchXParser,
                                                                           ParseFieldMatcher.STRICT);
                    SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parseContext,
                                                                                               searchRequestParsers.aggParsers,
                                                                                               searchRequestParsers.suggesters,
                                                                                               searchRequestParsers.searchExtParsers);
                    searchRequest.source(searchSourceBuilder);
                }

                Object includeHits = asMap.get("include_hits");
                if (includeHits != null) {
                    Loggers.getLogger(getClass()).warn("Request used deprecated 'include_hits' parameter.");
                    setIncludeHits(Boolean.parseBoolean(includeHits.toString()));
                }

                Object maxHits = asMap.get("max_hits");
                if (maxHits != null) {
                    setMaxHits(maxHits.toString());
                }
            } catch (Exception e) {
                String sSource = "_na_";
                try {
                    sSource = XContentHelper.convertToJson(source, false);
                } catch (Throwable e1) {
                    // ignore
                }
                throw new ClusteringException("Failed to parse source [" + sSource + "]", e);
            }
        }

        private void parseFieldSpecs(Map<String, List<String>> fieldSpecs) {
            for (Map.Entry<String, List<String>> e : fieldSpecs.entrySet()) {
                LogicalField logicalField = LogicalField.valueOfCaseInsensitive(e.getKey());
                if (logicalField != null) {
                    for (String fieldSpec : e.getValue()) {
                        addFieldMappingSpec(fieldSpec, logicalField);
                    }
                }
            }
        }

        /**
         * Map a hit's field to a logical section of a document to be clustered (title, content or URL).
         *
         * @see LogicalField
         */
        public ClusteringActionRequest addFieldMapping(String fieldName, LogicalField logicalField) {
            fieldMapping.add(new FieldMappingSpec(fieldName, logicalField, FieldSource.FIELD));
            return this;
        }

        /**
         * Map a hit's source field (field unpacked from the <code>_source</code> document)
         * to a logical section of a document to be clustered (title, content or URL).
         *
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
         *
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
                throw new ElasticsearchException("Field mapping specification must contain a " +
                        " valid source prefix for the field source: " + fieldSpec);
            }

            return this;
        }

        /**
         * Access to prepared field mapping.
         */
        List<FieldMappingSpec> getFieldMapping() {
            return fieldMapping;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (searchRequest == null) {
                validationException = addValidationError("No delegate search request",
                                                         validationException);
            }

            if (queryHint == null) {
                validationException = addValidationError("query hint may be empty but must not be null.",
                                                         validationException);
            }

            if (fieldMapping.isEmpty()) {
                validationException = addValidationError("At least one field should be mapped to a logical document field.",
                                                         validationException);
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
            out.writeInt(maxHits);

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
            this.maxHits = in.readInt();

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
            ClusteringActionRequestBuilder> {

        public ClusteringActionRequestBuilder(ElasticsearchClient client) {
            super(client, ClusteringAction.INSTANCE, new ClusteringActionRequest());
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

        public ClusteringActionRequestBuilder setSource(BytesReference content, SearchRequestParsers searchRequestParsers) {
            super.request.source(content, searchRequestParsers);
            return this;
        }

        /**
         * @deprecated Use {@link #setMaxHits} instead.
         */
        @Deprecated()
        public ClusteringActionRequestBuilder setIncludeHits(String includeHits) {
            if (includeHits != null)
                super.request.setIncludeHits(Boolean.parseBoolean(includeHits));
            else
                super.request.setIncludeHits(true);
            return this;
        }

        public ClusteringActionRequestBuilder setMaxHits(int maxHits) {
            super.request.setMaxHits(maxHits);
            return this;
        }

        public ClusteringActionRequestBuilder setMaxHits(String maxHits) {
            super.request.setMaxHits(maxHits);
            return this;
        }

        public ClusteringActionRequestBuilder addAttributes(Map<String, Object> attributes) {
            if (super.request.getAttributes() == null) {
                super.request.setAttributes(new HashMap<String, Object>());
            }
            super.request.getAttributes().putAll(attributes);
            return this;
        }

        public ClusteringActionRequestBuilder addAttribute(String key, Object value) {
            HashMap<String, Object> tmp = new HashMap<String, Object>();
            tmp.put(key, value);
            return addAttributes(tmp);
        }

        public ClusteringActionRequestBuilder setAttributes(Map<String, Object> attributes) {
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
    }

    /**
     * An {@link ActionResponse} for {@link ClusteringAction}.
     */
    public static class ClusteringActionResponse extends ActionResponse implements ToXContent {
        /**
         * Clustering-related response fields.
         */
        static final class Fields {
            static final String SEARCH_RESPONSE = "search_response";
            static final String CLUSTERS = "clusters";
            static final String INFO = "info";

            // from SearchResponse
            static final String _SCROLL_ID = "_scroll_id";
            static final String _SHARDS = "_shards";
            static final String TOTAL = "total";
            static final String SUCCESSFUL = "successful";
            static final String FAILED = "failed";
            static final String FAILURES = "failures";
            static final String STATUS = "status";
            static final String INDEX = "index";
            static final String SHARD = "shard";
            static final String REASON = "reason";
            static final String TOOK = "took";
            static final String TIMED_OUT = "timed_out";

            /**
             * {@link Fields#INFO} keys.
             */
            static final class Info {
                public static final String ALGORITHM = "algorithm";
                public static final String SEARCH_MILLIS = "search-millis";
                public static final String CLUSTERING_MILLIS = "clustering-millis";
                public static final String TOTAL_MILLIS = "total-millis";
                public static final String INCLUDE_HITS = "include-hits";
                public static final String MAX_HITS = "max-hits";
            }
        }

        private SearchResponse searchResponse;
        private DocumentGroup[] topGroups;
        private Map<String, String> info;

        ClusteringActionResponse() {
        }

        public ClusteringActionResponse(
                SearchResponse searchResponse,
                DocumentGroup[] topGroups,
                Map<String, String> info) {
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
                for (Map.Entry<String, String> e : info.entrySet()) {
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
            topGroups = new DocumentGroup[documentGroupsCount];
            for (int i = 0; i < documentGroupsCount; i++) {
                DocumentGroup group = new DocumentGroup();
                group.readFrom(in);
                topGroups[i] = group;
            }

            int entries = in.readVInt();
            info = new LinkedHashMap<>();
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
            ClusteringAction.ClusteringActionResponse> {
        private final Set<String> langCodeWarnings = new CopyOnWriteArraySet<>();

        private final TransportSearchAction searchAction;
        private final ControllerSingleton controllerSingleton;

        @Inject
        public TransportClusteringAction(Settings settings,
                                         ThreadPool threadPool,
                                         TransportService transportService,
                                         TransportSearchAction searchAction,
                                         ControllerSingleton controllerSingleton,
                                         ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings,
                  ClusteringAction.NAME,
                  threadPool,
                  actionFilters,
                  indexNameExpressionResolver,
                  transportService.getTaskManager());

            this.searchAction = searchAction;
            this.controllerSingleton = controllerSingleton;
            transportService.registerRequestHandler(
                    ClusteringAction.NAME,
                    ClusteringActionRequest::new,
                    ThreadPool.Names.SAME,
                    new TransportHandler());
        }

        @Override
        protected void doExecute(final ClusteringActionRequest clusteringRequest,
                                 final ActionListener<ClusteringActionResponse> listener) {
            final long tsSearchStart = System.nanoTime();
            searchAction.execute(clusteringRequest.getSearchRequest(), new ActionListener<SearchResponse>() {
                @Override
                public void onFailure(Exception e) {
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
                            listener.onFailure(new IllegalArgumentException("No such algorithm: " + algorithmId));
                            return;
                        }
                    }
                    final String _algorithmId = algorithmId;

                    /*
                     * We're not a threaded listener so we're running on the search thread. This
                     * is good -- we don't want to serve more clustering requests than we can handle
                     * anyway. 
                     */
                    final Controller controller = controllerSingleton.getController();

                    final Map<String, Object> processingAttrs = new HashMap<>();
                    Map<String, Object> requestAttrs = clusteringRequest.getAttributes();
                    if (requestAttrs != null) {
                        handleInputClustersSpec(requestAttrs);
                        processingAttrs.putAll(requestAttrs);
                    }

                    try {
                        CommonAttributesDescriptor.attributeBuilder(processingAttrs)
                                .documents(prepareDocumentsForClustering(clusteringRequest, response))
                                .query(clusteringRequest.getQueryHint());

                        final long tsClusteringStart = System.nanoTime();
                        final ProcessingResult result = AccessController.doPrivileged((PrivilegedAction<ProcessingResult>) () ->
                                                                                      controller.process(processingAttrs, _algorithmId));
                        final DocumentGroup[] groups = adapt(result.getClusters());
                        final long tsClusteringEnd = System.nanoTime();

                        final Map<String, String> info = new LinkedHashMap<>();
                        info.put(ClusteringActionResponse.Fields.Info.ALGORITHM,
                                 algorithmId);
                        info.put(ClusteringActionResponse.Fields.Info.SEARCH_MILLIS,
                                 Long.toString(TimeUnit.NANOSECONDS.toMillis(tsSearchEnd - tsSearchStart)));
                        info.put(ClusteringActionResponse.Fields.Info.CLUSTERING_MILLIS,
                                 Long.toString(TimeUnit.NANOSECONDS.toMillis(tsClusteringEnd - tsClusteringStart)));
                        info.put(ClusteringActionResponse.Fields.Info.TOTAL_MILLIS,
                                 Long.toString(TimeUnit.NANOSECONDS.toMillis(tsClusteringEnd - tsSearchStart)));
                        info.put(ClusteringActionResponse.Fields.Info.INCLUDE_HITS,
                                 Boolean.toString(clusteringRequest.getIncludeHits()));
                        info.put(ClusteringActionResponse.Fields.Info.MAX_HITS,
                                 clusteringRequest.getMaxHits() == Integer.MAX_VALUE ?
                                         "" : Integer.toString(clusteringRequest.getMaxHits()));

                        // Trim search response's hits if we need to.
                        if (clusteringRequest.getMaxHits() != Integer.MAX_VALUE) {
                            response = filterMaxHits(response, clusteringRequest.getMaxHits());
                        }

                        listener.onResponse(new ClusteringActionResponse(response, groups, info));
                    } catch (ProcessingException e) {
                        // Log a full stack trace with all nested exceptions but only return 
                        // ElasticSearchException exception with a simple String (otherwise 
                        // clients cannot deserialize exception classes).
                        String message = "Search results clustering error: " + e.getMessage();
                        listener.onFailure(new ElasticsearchException(message));

                        logger.warn("Could not process clustering request.", e);
                        return;
                    }
                }
            });
        }

        @SuppressWarnings("unchecked")
        private void handleInputClustersSpec(Map<String, Object> requestAttrs) {
            // Handle the "special" attribute key "clusters", which Lingo3G recognizes as a request
            // for incremental clustering. The structure of the input clusters must follow this xcontent
            // schema:
            //
            // "clusters": [{}, {}, ...]
            //
            // with zero, one or more objects representing cluster labels inside:
            //
            // { "label": "cluster label",
            //   "subclusters": [{}, {}, ...] }
            //
            // There is very limited input validation; this feature is largerly undocumented and
            // officially unsupported.
            if (requestAttrs.containsKey("clusters")) {
                requestAttrs.put("clusters", parseClusters((List<Object>) requestAttrs.get("clusters")));
            }
        }

        @SuppressWarnings("unchecked")
        private List<Cluster> parseClusters(List<Object> xcontentList) {
            ArrayList<Cluster> result = new ArrayList<>();
            for (Object xcontent : xcontentList) {
                result.add(parseCluster((Map<String, Object>) xcontent));
            }
            return result;
        }

        @SuppressWarnings("unchecked")
        private Cluster parseCluster(Map<String, Object> xcontent) {
            String label = (String) xcontent.get("label");
            Cluster cluster = new Cluster(label);
            List<Object> subclusters = (List<Object>) xcontent.get("clusters");
            if (subclusters != null) {
                cluster = cluster.addSubclusters(parseClusters(subclusters));
            }
            return cluster;
        }

        protected SearchResponse filterMaxHits(SearchResponse response, int maxHits) {
            // We will use internal APIs here for efficiency. The plugin has restricted explicit ES compatibility
            // anyway. Alternatively, we could serialize/ filter/ deserialize JSON, but this seems simpler.
            SearchHits allHits = response.getHits();
            InternalSearchHit[] trimmedHits = new InternalSearchHit[Math.min(maxHits, allHits.hits().length)];
            System.arraycopy(allHits.hits(), 0, trimmedHits, 0, trimmedHits.length);

            InternalAggregations _internalAggregations = null;
            if (response.getAggregations() != null) {
                _internalAggregations = new InternalAggregations(toInternal(response.getAggregations().asList()));
            }

            InternalSearchHits _searchHits =
                    new InternalSearchHits(trimmedHits, allHits.getTotalHits(), allHits.getMaxScore());

            SearchProfileShardResults _searchProfileShardResults = new SearchProfileShardResults(response.getProfileResults());

            InternalSearchResponse _searchResponse =
                    new InternalSearchResponse(
                            _searchHits,
                            _internalAggregations,
                            response.getSuggest(),
                            _searchProfileShardResults,
                            response.isTimedOut(),
                            response.isTerminatedEarly());

            return new SearchResponse(
                    _searchResponse,
                    response.getScrollId(),
                    response.getTotalShards(),
                    response.getSuccessfulShards(),
                    response.getTookInMillis(),
                    response.getShardFailures());
        }

        private List<InternalAggregation> toInternal(List<Aggregation> list) {
            List<InternalAggregation> t = new ArrayList<>(list.size());
            for (Aggregation a : list) {
                t.add((InternalAggregation) a);
            }
            return t;
        }

        /* */
        protected DocumentGroup[] adapt(List<Cluster> clusters) {
            DocumentGroup[] groups = new DocumentGroup[clusters.size()];
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
            subclusters = (subclusters == null ? Collections.emptyList() : subclusters);
            group.setSubgroups(adapt(subclusters));

            return group;
        }

        /**
         * Map {@link SearchHit} fields to logical fields of Carrot2 {@link Document}.
         */
        private List<Document> prepareDocumentsForClustering(
                final ClusteringActionRequest request,
                SearchResponse response) {
            SearchHit[] hits = response.getHits().hits();
            List<Document> documents = new ArrayList<>(hits.length);
            List<FieldMappingSpec> fieldMapping = request.getFieldMapping();
            StringBuilder title = new StringBuilder();
            StringBuilder content = new StringBuilder();
            StringBuilder url = new StringBuilder();
            StringBuilder language = new StringBuilder();
            boolean emptySourceWarningEmitted = false;

            for (SearchHit hit : hits) {
                // Prepare logical fields for each hit.
                title.setLength(0);
                content.setLength(0);
                url.setLength(0);
                language.setLength(0);

                Map<String, SearchHitField> fields = hit.getFields();
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();

                Map<String, Object> sourceAsMap = null;
                for (FieldMappingSpec spec : fieldMapping) {
                    // Determine the content source.
                    Object appendContent = null;
                    outer:
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
                                appendContent = join(Arrays.asList(highlightField.fragments()));
                            }
                            break;

                        case SOURCE:
                            if (sourceAsMap == null) {
                                if (!hit.hasSource()) {
                                    if (!emptySourceWarningEmitted) {
                                        emptySourceWarningEmitted = true;
                                        logger.warn("_source field mapping used but no source available for: {}, field {}",
                                                    hit.getId(),
                                                    spec.field);
                                    }
                                } else {
                                    sourceAsMap = hit.getSource();
                                }
                            }

                            if (sourceAsMap != null) {
                                String[] fieldNames = spec.field.split("\\.");
                                Object value = sourceAsMap;

                                // Descend into maps.
                                for (String fieldName : fieldNames) {
                                    if (Map.class.isInstance(value)) {
                                        value = ((Map<?, ?>) value).get(fieldName);
                                        if (value == null) {
                                            // No such key.
                                            logger.warn("Cannot find into field {} from spec: {}",
                                                    fieldName,
                                                    spec.field);
                                            break outer;
                                        }
                                    } else {
                                        logger.warn("Field is not a map: {} in spec.: {}",
                                                fieldName,
                                                spec.field);
                                        break outer;
                                    }
                                }

                                if (value instanceof List) {
                                    appendContent = join((List<?>) value);
                                } else {
                                    appendContent = value;
                                }
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

        static String join(List<?> list) {
            StringBuilder sb = new StringBuilder();
            for (Object t : list) {
                if (sb.length() > 0) {
                    sb.append(" . ");
                }
                sb.append(t != null ? t.toString() : "");
            }
            return sb.toString();
        }

        private final class TransportHandler implements TransportRequestHandler<ClusteringActionRequest> {
            @Override
            public void messageReceived(final ClusteringActionRequest request, final TransportChannel channel) throws Exception {
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
                    public void onFailure(Exception e) {
                        try {
                            channel.sendResponse(e);
                        } catch (Exception e1) {
                            logger.warn("Failed to send error response for action ["
                                    + ClusteringAction.NAME + "] and request [" + request + "]", e1);
                        }
                    }
                });
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

        private final SearchRequestParsers searchRequestParsers;

        @Inject
        public RestClusteringAction(
                Settings settings,
                SearchRequestParsers searchRequestParsers,
                RestController controller) {
            super(settings);

            this.searchRequestParsers = searchRequestParsers;

            controller.registerHandler(POST, "/" + NAME, this);
            controller.registerHandler(POST, "/{index}/" + NAME, this);
            controller.registerHandler(POST, "/{index}/{type}/" + NAME, this);

            controller.registerHandler(GET, "/" + NAME, this);
            controller.registerHandler(GET, "/{index}/" + NAME, this);
            controller.registerHandler(GET, "/{index}/{type}/" + NAME, this);
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            // A POST request must have a body.
            if (request.method() == POST && !request.hasContent()) {
                return channel -> emitErrorResponse(channel, logger,
                        new IllegalArgumentException("Request body was expected for a POST request."));
            }

            // Contrary to ES's default search handler we will not support
            // GET requests with a body (this is against HTTP spec guidance 
            // in my opinion -- GET requests should be URL-based). 
            if (request.method() == GET && request.hasContent()) {
                return channel -> emitErrorResponse(channel, logger,
                        new IllegalArgumentException("Request body was unexpected for a GET request."));
            }

            // Build an action request with data from the request.

            // Parse incoming arguments depending on the HTTP method used to make
            // the request.
            final ClusteringActionRequestBuilder actionBuilder = new ClusteringActionRequestBuilder(client);
            SearchRequest searchRequest = new SearchRequest();
            switch (request.method()) {
                case POST:
                    searchRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
                    searchRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
                    actionBuilder.setSearchRequest(searchRequest);
                    actionBuilder.setSource(request.content(), searchRequestParsers);
                    break;

                case GET:
                    RestSearchAction.parseSearchRequest(searchRequest, request, searchRequestParsers, parseFieldMatcher, null);
                    actionBuilder.setSearchRequest(searchRequest);
                    fillFromGetRequest(actionBuilder, request);
                    break;

                default:
                    throw org.carrot2.elasticsearch.Preconditions.unreachable();
            }

            // Dispatch clustering request.
            return channel -> client.execute(ClusteringAction.INSTANCE, actionBuilder.request(),
                    new ActionListener<ClusteringActionResponse>() {
                        @Override
                        public void onResponse(ClusteringActionResponse response) {
                            try {
                                XContentBuilder builder = channel.newBuilder();
                                builder.startObject();
                                response.toXContent(builder, request);
                                builder.endObject();
                                channel.sendResponse(
                                        new BytesRestResponse(
                                                response.getSearchResponse().status(),
                                                builder));
                            } catch (Exception e) {
                                logger.debug("Failed to emit response.", e);
                                onFailure(e);
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            emitErrorResponse(channel, logger, e);
                        }
                    });
        }

        private static final EnumMap<LogicalField, String> GET_REQUEST_FIELDMAPPERS;

        static {
            GET_REQUEST_FIELDMAPPERS = new EnumMap<>(LogicalField.class);
            for (LogicalField lf : LogicalField.values()) {
                GET_REQUEST_FIELDMAPPERS.put(lf, "field_mapping_" + lf.name().toLowerCase(Locale.ROOT));
            }
        }

        /**
         * Extract and parse HTTP GET parameters for the clustering request.
         */
        private void fillFromGetRequest(
                ClusteringActionRequestBuilder actionBuilder,
                RestRequest request) {
            // Use the search query as the query hint, if explicit query hint
            // is not available. 
            if (request.hasParam("query_hint")) {
                actionBuilder.setQueryHint(request.param("query_hint"));
            } else {
                actionBuilder.setQueryHint(request.param("q"));
            }

            // Algorithm.
            if (request.hasParam("algorithm")) {
                actionBuilder.setAlgorithm(request.param("algorithm"));
            }

            // include_hits
            if (request.hasParam("include_hits")) {
                actionBuilder.setIncludeHits(request.param("include_hits"));
            }

            // max_hits
            if (request.hasParam("max_hits")) {
                actionBuilder.setMaxHits(request.param("max_hits"));
            }

            // Field mappers.
            for (Map.Entry<LogicalField, String> e : GET_REQUEST_FIELDMAPPERS.entrySet()) {
                if (request.hasParam(e.getValue())) {
                    for (String spec : Strings.splitStringByCommaToArray(request.param(e.getValue()))) {
                        actionBuilder.addFieldMappingSpec(spec, e.getKey());
                    }
                }
            }
        }
    }
}
