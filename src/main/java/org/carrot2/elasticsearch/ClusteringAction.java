package org.carrot2.elasticsearch;

import static org.carrot2.elasticsearch.LoggerUtils.emitErrorResponse;
import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.carrot2.attrs.Attrs;
import org.carrot2.clustering.Cluster;
import org.carrot2.clustering.ClusteringAlgorithm;
import org.carrot2.clustering.ClusteringAlgorithmProvider;
import org.carrot2.clustering.Document;
import org.carrot2.language.LanguageComponents;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

/** Perform clustering of search results. */
public class ClusteringAction extends ActionType<ClusteringAction.ClusteringActionResponse> {
  /* Action name. */
  public static final String NAME = "indices:data/read/cluster";

  /* Reusable singleton. */
  public static final ClusteringAction INSTANCE = new ClusteringAction();

  private ClusteringAction() {
    super(NAME, ClusteringActionResponse::new);
  }

  @Override
  public Writeable.Reader<ClusteringActionResponse> getResponseReader() {
    return ClusteringActionResponse::new;
  }

  /** An {@link ActionRequest} for {@link ClusteringAction}. */
  public static class ClusteringActionRequest extends ActionRequest
      implements IndicesRequest.Replaceable {
    public static String JSON_QUERY_HINT = "query_hint";
    public static String JSON_FIELD_MAPPING = "field_mapping";
    public static String JSON_ALGORITHM = "algorithm";
    public static String JSON_ATTRIBUTES = "attributes";
    public static String JSON_SEARCH_REQUEST = "search_request";
    public static String JSON_MAX_HITS = "max_hits";
    public static String JSON_CREATE_UNGROUPED_CLUSTER = "create_ungrouped";
    public static String JSON_LANGUAGE = "language";

    private SearchRequest searchRequest;
    private String queryHint;
    private List<FieldMappingSpec> fieldMapping = new ArrayList<>();
    private String algorithm;
    private int maxHits = Integer.MAX_VALUE;
    private Map<String, Object> attributes;
    private boolean createUngroupedDocumentsCluster;
    private String defaultLanguage = "English";

    /**
     * Set the {@link SearchRequest} to use for fetching documents to be clustered. The search
     * request must fetch enough documents for clustering to make sense (set <code>size</code>
     * appropriately).
     *
     * @param searchRequest search request to set
     * @return same builder instance
     */
    public ClusteringActionRequest setSearchRequest(SearchRequest searchRequest) {
      this.searchRequest = searchRequest;
      return this;
    }

    /**
     * @see #setSearchRequest(SearchRequest)
     * @param builder The search builder
     * @return Returns same object for chaining.
     */
    public ClusteringActionRequest setSearchRequest(SearchRequestBuilder builder) {
      return setSearchRequest(builder.request());
    }

    ClusteringActionRequest() {}

    public ClusteringActionRequest(StreamInput in) throws IOException {
      SearchRequest searchRequest = new SearchRequest(in);

      this.searchRequest = searchRequest;
      this.queryHint = in.readOptionalString();
      this.algorithm = in.readOptionalString();
      this.maxHits = in.readInt();
      this.createUngroupedDocumentsCluster = in.readBoolean();
      this.defaultLanguage = in.readString();

      int count = in.readVInt();
      while (count-- > 0) {
        FieldMappingSpec spec = new FieldMappingSpec(in);
        fieldMapping.add(spec);
      }

      boolean hasAttributes = in.readBoolean();
      if (hasAttributes) {
        attributes = in.readMap();
      }
    }

    public SearchRequest getSearchRequest() {
      return searchRequest;
    }

    /**
     * @param queryHint A set of terms which correspond to the query. This hint helps the clustering
     *     algorithm to avoid trivial clusters around the query terms. Typically the query terms
     *     hint will be identical to what the user typed in the search box.
     *     <p>The hint may be an empty string but must not be <code>null</code>.
     * @return same builder instance
     */
    public ClusteringActionRequest setQueryHint(String queryHint) {
      this.queryHint = queryHint;
      return this;
    }

    /**
     * @see #setQueryHint(String)
     * @return Query hint
     */
    public String getQueryHint() {
      return queryHint;
    }

    /**
     * Sets the identifier of the clustering algorithm to use. If <code>null</code>, the default
     * algorithm will be used (depending on what's available).
     *
     * @param algorithm identifier of the clustering algorithm to use.
     * @return Same object for chaining
     */
    public ClusteringActionRequest setAlgorithm(String algorithm) {
      this.algorithm = algorithm;
      return this;
    }

    /**
     * @see #setAlgorithm
     * @return The current algorithm to use for clustering
     */
    public String getAlgorithm() {
      return algorithm;
    }

    /**
     * Sets the maximum number of hits to return with the response. Setting this value to zero will
     * only return clusters, without any hits (can be used to save bandwidth if only cluster labels
     * are needed).
     *
     * <p>Set to {@link Integer#MAX_VALUE} to include all the hits.
     *
     * @param maxHits Maximum hits
     */
    public void setMaxHits(int maxHits) {
      assert maxHits >= 0;
      this.maxHits = maxHits;
    }

    /**
     * Sets {@link #setMaxHits(int)} from a string. An empty string or null means all hits should be
     * included.
     *
     * @param value Maximum number of hits.
     */
    public void setMaxHits(String value) {
      if (value == null || value.trim().isEmpty()) {
        setMaxHits(Integer.MAX_VALUE);
      } else {
        setMaxHits(Integer.parseInt(value));
      }
    }

    /**
     * @return Returns the maximum number of hits to be returned as part of the response. * If equal
     *     to {@link Integer#MAX_VALUE}, then all hits will be returned.
     */
    public int getMaxHits() {
      return maxHits;
    }

    /**
     * Sets a map of runtime override attributes for clustering algorithms.
     *
     * @param map Clustering attributes to use.
     * @return Same object for chaining
     */
    public ClusteringActionRequest setAttributes(Map<String, Object> map) {
      this.attributes = map;
      return this;
    }

    /**
     * @see #setAttributes(Map)
     * @return Clustering algorithm attributes map
     */
    public Map<String, Object> getAttributes() {
      return attributes;
    }

    /**
     * Parses some {@link org.elasticsearch.common.xcontent.XContent} and fills in the request.
     *
     * @param source arg
     * @param xContentType arg
     * @param xContentRegistry arg
     */
    @SuppressWarnings("unchecked")
    public void source(
        BytesReference source, XContentType xContentType, NamedXContentRegistry xContentRegistry) {
      if (source == null || source.length() == 0) {
        return;
      }

      try (XContentParser parser =
          XContentHelper.createParser(
              xContentRegistry,
              DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
              source,
              xContentType)) {
        // We should avoid reparsing search_request here
        // but it's terribly difficult to slice the underlying byte
        // buffer to get just the search request.
        Map<String, Object> asMap = parser.mapOrdered();

        Boolean createUngrouped = (Boolean) asMap.get(JSON_CREATE_UNGROUPED_CLUSTER);
        if (createUngrouped != null) {
          setCreateUngroupedDocumentsCluster(createUngrouped);
        }

        String queryHint = (String) asMap.get(JSON_QUERY_HINT);
        if (queryHint != null) {
          setQueryHint(queryHint);
        }

        String defaultLanguage = (String) asMap.get(JSON_LANGUAGE);
        if (defaultLanguage != null) {
          setDefaultLanguage(defaultLanguage);
        }

        Map<String, List<String>> fieldMapping =
            (Map<String, List<String>>) asMap.get(JSON_FIELD_MAPPING);
        if (fieldMapping != null) {
          parseFieldSpecs(fieldMapping);
        }

        String algorithm = (String) asMap.get(JSON_ALGORITHM);
        if (algorithm != null) {
          setAlgorithm(algorithm);
        }

        Map<String, Object> attributes = (Map<String, Object>) asMap.get(JSON_ATTRIBUTES);
        if (attributes != null) {
          setAttributes(attributes);
        }

        Map<String, ?> searchRequestMap = (Map<String, ?>) asMap.get(JSON_SEARCH_REQUEST);
        if (searchRequestMap != null) {
          if (this.searchRequest == null) {
            searchRequest = new SearchRequest();
          }

          XContentBuilder builder =
              XContentFactory.contentBuilder(XContentType.JSON).map(searchRequestMap);
          XContentParser searchXParser =
              XContentFactory.xContent(XContentType.JSON)
                  .createParser(
                      xContentRegistry,
                      DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                      Strings.toString(builder));
          SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(searchXParser);
          searchRequest.source(searchSourceBuilder);
        }

        Object maxHits = asMap.get(JSON_MAX_HITS);
        if (maxHits != null) {
          setMaxHits(maxHits.toString());
        }
      } catch (Exception e) {
        String sSource = "_na_";
        try {
          sSource = XContentHelper.convertToJson(source, false, false, xContentType);
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
     * @param fieldName field name
     * @param logicalField logical field mapping.
     * @return Same object for chaining
     */
    public ClusteringActionRequest addFieldMapping(String fieldName, LogicalField logicalField) {
      fieldMapping.add(new FieldMappingSpec(fieldName, logicalField, FieldSource.FIELD));
      return this;
    }

    /**
     * Map a hit's source field (field unpacked from the <code>_source</code> document) to a logical
     * section of a document to be clustered (title, content or URL).
     *
     * @see LogicalField
     * @param sourceFieldName field name
     * @param logicalField logical field mapping.
     * @return Same object for chaining
     */
    public ClusteringActionRequest addSourceFieldMapping(
        String sourceFieldName, LogicalField logicalField) {
      fieldMapping.add(new FieldMappingSpec(sourceFieldName, logicalField, FieldSource.SOURCE));
      return this;
    }

    /**
     * Map a hit's highligted field (fragments of the original field) to a logical section of a
     * document to be clustered. This may be used to decrease the amount of information passed to
     * the clustering engine but also to "focus" the clustering engine on the context of the query.
     *
     * @param fieldName field name
     * @param logicalField logical field mapping.
     * @return Same object for chaining
     */
    public ClusteringActionRequest addHighlightedFieldMapping(
        String fieldName, LogicalField logicalField) {
      fieldMapping.add(new FieldMappingSpec(fieldName, logicalField, FieldSource.HIGHLIGHT));
      return this;
    }

    /**
     * Add a (valid!) field mapping specification to a logical field.
     *
     * @see FieldSource
     * @param fieldSpec field specification
     * @param logicalField logical field mapping.
     * @return Same object for chaining
     */
    public ClusteringActionRequest addFieldMappingSpec(
        String fieldSpec, LogicalField logicalField) {
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
        throw new ElasticsearchException(
            "Field mapping specification must contain a "
                + " valid source prefix for the field source: "
                + fieldSpec);
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
        validationException =
            addValidationError(
                "query hint may be empty but must not be null.", validationException);
      }

      if (fieldMapping.isEmpty()) {
        validationException =
            addValidationError(
                "At least one field should be mapped to a logical document field.",
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
      out.writeBoolean(createUngroupedDocumentsCluster);
      out.writeString(defaultLanguage);

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
    public IndicesRequest indices(String... strings) {
      return searchRequest.indices(strings);
    }

    @Override
    public String[] indices() {
      return searchRequest.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
      return searchRequest.indicesOptions();
    }

    public void setCreateUngroupedDocumentsCluster(boolean enabled) {
      this.createUngroupedDocumentsCluster = enabled;
    }

    public void setDefaultLanguage(String defaultLanguage) {
      this.defaultLanguage = Objects.requireNonNull(defaultLanguage);
    }

    public String getDefaultLanguage() {
      return defaultLanguage;
    }
  }

  /** An {@link ActionRequestBuilder} for {@link ClusteringAction}. */
  public static class ClusteringActionRequestBuilder
      extends ActionRequestBuilder<ClusteringActionRequest, ClusteringActionResponse> {

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

    public ClusteringActionRequestBuilder setSource(
        BytesReference content, XContentType xContentType, NamedXContentRegistry xContentRegistry) {
      super.request.source(content, xContentType, xContentRegistry);
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

    public ClusteringActionRequestBuilder addFieldMapping(
        String fieldName, LogicalField logicalField) {
      super.request.addFieldMapping(fieldName, logicalField);
      return this;
    }

    public ClusteringActionRequestBuilder addSourceFieldMapping(
        String fieldName, LogicalField logicalField) {
      super.request.addSourceFieldMapping(fieldName, logicalField);
      return this;
    }

    public ClusteringActionRequestBuilder addHighlightedFieldMapping(
        String fieldName, LogicalField logicalField) {
      super.request.addHighlightedFieldMapping(fieldName, logicalField);
      return this;
    }

    public ClusteringActionRequestBuilder addFieldMappingSpec(
        String fieldSpec, LogicalField logicalField) {
      super.request.addFieldMappingSpec(fieldSpec, logicalField);
      return this;
    }

    public ClusteringActionRequestBuilder setCreateUngroupedDocumentsCluster(boolean enabled) {
      super.request.setCreateUngroupedDocumentsCluster(enabled);
      return this;
    }

    public ClusteringActionRequestBuilder setDefaultLanguage(String language) {
      super.request.setDefaultLanguage(language);
      return this;
    }
  }

  /** An {@link ActionResponse} for {@link ClusteringAction}. */
  public static class ClusteringActionResponse extends ActionResponse implements ToXContent {
    /** Clustering-related response fields. */
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

      /** {@link Fields#INFO} keys. */
      static final class Info {
        public static final String ALGORITHM = "algorithm";
        public static final String SEARCH_MILLIS = "search-millis";
        public static final String CLUSTERING_MILLIS = "clustering-millis";
        public static final String TOTAL_MILLIS = "total-millis";
        public static final String INCLUDE_HITS = "include-hits";
        public static final String MAX_HITS = "max-hits";
        public static final String LANGUAGES = "languages";
      }
    }

    private SearchResponse searchResponse;
    private DocumentGroup[] topGroups;
    private Map<String, String> info;

    ClusteringActionResponse(StreamInput in) throws IOException {
      boolean hasSearchResponse = in.readBoolean();
      if (hasSearchResponse) {
        this.searchResponse = new SearchResponse(in);
      }

      int documentGroupsCount = in.readVInt();
      topGroups = new DocumentGroup[documentGroupsCount];
      for (int i = 0; i < documentGroupsCount; i++) {
        DocumentGroup group = new DocumentGroup(in);
        topGroups[i] = group;
      }

      int entries = in.readVInt();
      info = new LinkedHashMap<>();
      for (int i = 0; i < entries; i++) {
        info.put(in.readOptionalString(), in.readOptionalString());
      }
    }

    public ClusteringActionResponse(
        SearchResponse searchResponse, DocumentGroup[] topGroups, Map<String, String> info) {
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
      if (searchResponse != null) {
        searchResponse.innerToXContent(builder, ToXContent.EMPTY_PARAMS);
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
    public String toString() {
      return ToString.objectToJson(this);
    }
  }

  /** A {@link TransportAction} for {@link ClusteringAction}. */
  public static class TransportClusteringAction
      extends TransportAction<
          ClusteringAction.ClusteringActionRequest, ClusteringAction.ClusteringActionResponse> {
    protected Logger logger = LogManager.getLogger(getClass());

    private final Set<String> langCodeWarnings = new CopyOnWriteArraySet<>();

    private final TransportSearchAction searchAction;
    private final ClusteringContext context;

    @Inject
    public TransportClusteringAction(
        TransportService transportService,
        TransportSearchAction searchAction,
        ClusteringContext controllerSingleton,
        ActionFilters actionFilters) {
      super(ClusteringAction.NAME, actionFilters, transportService.getTaskManager());

      this.searchAction = searchAction;
      this.context = controllerSingleton;
      transportService.registerRequestHandler(
          ClusteringAction.NAME,
          ThreadPool.Names.SAME,
          ClusteringActionRequest::new,
          new TransportHandler());
    }

    @Override
    protected void doExecute(
        Task task,
        final ClusteringActionRequest clusteringRequest,
        final ActionListener<ClusteringActionResponse> listener) {
      final long tsSearchStart = System.nanoTime();
      searchAction.execute(
          clusteringRequest.getSearchRequest(),
          new ActionListener<SearchResponse>() {
            @Override
            public void onFailure(Exception e) {
              listener.onFailure(e);
            }

            @Override
            public void onResponse(SearchResponse response) {
              final long tsSearchEnd = System.nanoTime();

              LinkedHashMap<String, ClusteringAlgorithmProvider> algorithms =
                  context.getAlgorithms();

              final String algorithmId =
                  requireNonNullElse(
                      clusteringRequest.getAlgorithm(), algorithms.keySet().iterator().next());

              ClusteringAlgorithmProvider provider = algorithms.get(algorithmId);
              if (provider == null) {
                listener.onFailure(
                    new IllegalArgumentException("No such algorithm: " + algorithmId));
                return;
              }

              /*
               * We're not a threaded listener so we're running on the search thread. This
               * is good -- we don't want to serve more clustering requests than we can handle
               * anyway.
               */
              ClusteringAlgorithm algorithm = provider.get();

              try {
                Map<String, Object> requestAttrs = clusteringRequest.getAttributes();
                if (requestAttrs != null) {
                  Attrs.populate(algorithm, requestAttrs);
                }

                String queryHint = clusteringRequest.getQueryHint();
                if (queryHint != null) {
                  algorithm.accept(
                      new OptionalQueryHintSetterVisitor(clusteringRequest.getQueryHint()));
                }

                List<InputDocument> documents =
                    prepareDocumentsForClustering(clusteringRequest, response);

                String defaultLanguage = clusteringRequest.getDefaultLanguage();
                if (!context.isLanguageSupported(defaultLanguage)) {
                  throw new RuntimeException(
                      "The requested default language is not supported: '" + defaultLanguage + "'");
                }

                // Split documents into language groups.
                Map<String, List<InputDocument>> documentsByLanguage =
                    documents.stream()
                        .collect(
                            Collectors.groupingBy(
                                doc -> {
                                  String lang = doc.language();
                                  return lang == null ? defaultLanguage : lang;
                                }));

                // Run clustering.
                long tsClusteringTotal = 0;
                HashSet<String> warnOnce = new HashSet<>();
                LinkedHashMap<String, List<Cluster<InputDocument>>> clustersByLanguage =
                    new LinkedHashMap<>();
                for (Map.Entry<String, List<InputDocument>> e : documentsByLanguage.entrySet()) {
                  String lang = e.getKey();
                  if (!context.isLanguageSupported(lang)) {
                    if (warnOnce.add(lang)) {
                      logger.warn(
                          "Language is not supported, documents in this "
                              + "language will not be clustered: '"
                              + lang
                              + "'");
                    }
                  } else {
                    LanguageComponents languageComponents = context.getLanguageComponents(lang);
                    final long tsClusteringStart = System.nanoTime();
                    clustersByLanguage.put(
                        lang, algorithm.cluster(e.getValue().stream(), languageComponents));
                    final long tsClusteringEnd = System.nanoTime();
                    tsClusteringTotal += (tsClusteringEnd - tsClusteringStart);
                  }
                }

                final Map<String, String> info = new LinkedHashMap<>();
                info.put(ClusteringActionResponse.Fields.Info.ALGORITHM, algorithmId);
                info.put(
                    ClusteringActionResponse.Fields.Info.SEARCH_MILLIS,
                    Long.toString(TimeUnit.NANOSECONDS.toMillis(tsSearchEnd - tsSearchStart)));
                info.put(
                    ClusteringActionResponse.Fields.Info.CLUSTERING_MILLIS,
                    Long.toString(TimeUnit.NANOSECONDS.toMillis(tsClusteringTotal)));
                info.put(
                    ClusteringActionResponse.Fields.Info.TOTAL_MILLIS,
                    Long.toString(
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - tsSearchStart)));
                info.put(
                    ClusteringActionResponse.Fields.Info.MAX_HITS,
                    clusteringRequest.getMaxHits() == Integer.MAX_VALUE
                        ? ""
                        : Integer.toString(clusteringRequest.getMaxHits()));
                info.put(
                    ClusteringActionResponse.Fields.Info.LANGUAGES,
                    String.join(", ", clustersByLanguage.keySet()));

                // Trim search response's hits if we need to.
                if (clusteringRequest.getMaxHits() != Integer.MAX_VALUE) {
                  response = filterMaxHits(response, clusteringRequest.getMaxHits());
                }

                AtomicInteger groupId = new AtomicInteger();
                Map<String, DocumentGroup[]> adaptedByLanguage =
                    clustersByLanguage.entrySet().stream()
                        .filter(e -> !e.getValue().isEmpty())
                        .collect(
                            Collectors.toMap(Map.Entry::getKey, e -> adapt(e.getValue(), groupId)));

                final ArrayList<DocumentGroup> groups = new ArrayList<>();
                adaptedByLanguage
                    .values()
                    .forEach(langClusters -> groups.addAll(Arrays.asList(langClusters)));

                if (adaptedByLanguage.size() > 1) {
                  groups.sort(
                      (a, b) ->
                          Integer.compare(b.uniqueDocuments().size(), a.uniqueDocuments().size()));
                }

                if (clusteringRequest.createUngroupedDocumentsCluster) {
                  DocumentGroup ungrouped = new DocumentGroup();
                  ungrouped.setId(groupId.incrementAndGet());
                  ungrouped.setPhrases(new String[] {"Ungrouped documents"});
                  ungrouped.setUngroupedDocuments(true);
                  ungrouped.setScore(0d);

                  LinkedHashSet<InputDocument> ungroupedDocuments = new LinkedHashSet<>(documents);
                  clustersByLanguage
                      .values()
                      .forEach(langClusters -> removeReferenced(ungroupedDocuments, langClusters));
                  ungrouped.setDocumentReferences(
                      ungroupedDocuments.stream()
                          .map(InputDocument::getStringId)
                          .toArray(String[]::new));

                  groups.add(ungrouped);
                }

                listener.onResponse(
                    new ClusteringActionResponse(
                        response, groups.toArray(new DocumentGroup[0]), info));
              } catch (Exception e) {
                // Log a full stack trace with all nested exceptions but only return
                // ElasticSearchException exception with a simple String (otherwise
                // clients cannot deserialize exception classes).
                String message = "Clustering error: " + e.getMessage();
                logger.warn(message, e);
                listener.onFailure(new ElasticsearchException(message));
              }
            }

            private void removeReferenced(
                LinkedHashSet<InputDocument> ungrouped, List<Cluster<InputDocument>> clusters) {
              clusters.forEach(
                  cluster -> {
                    ungrouped.removeAll(cluster.getDocuments());
                    removeReferenced(ungrouped, cluster.getClusters());
                  });
            }
          });
    }

    public static <T> T requireNonNullElse(T first, T def) {
      return first != null ? first : def;
    }

    protected SearchResponse filterMaxHits(SearchResponse response, int maxHits) {
      // We will use internal APIs here for efficiency. The plugin has restricted explicit ES
      // compatibility
      // anyway. Alternatively, we could serialize/ filter/ deserialize JSON, but this seems
      // simpler.
      SearchHits allHits = response.getHits();
      SearchHit[] trimmedHits = new SearchHit[Math.min(maxHits, allHits.getHits().length)];
      System.arraycopy(allHits.getHits(), 0, trimmedHits, 0, trimmedHits.length);

      InternalAggregations _internalAggregations = null;
      if (response.getAggregations() != null) {
        _internalAggregations =
            new InternalAggregations(toInternal(response.getAggregations().asList()), null);
      }

      SearchHits _searchHits =
          new SearchHits(trimmedHits, allHits.getTotalHits(), allHits.getMaxScore());

      SearchProfileShardResults _searchProfileShardResults =
          new SearchProfileShardResults(response.getProfileResults());

      InternalSearchResponse _searchResponse =
          new InternalSearchResponse(
              _searchHits,
              _internalAggregations,
              response.getSuggest(),
              _searchProfileShardResults,
              response.isTimedOut(),
              response.isTerminatedEarly(),
              response.getNumReducePhases());

      return new SearchResponse(
          _searchResponse,
          response.getScrollId(),
          response.getTotalShards(),
          response.getSuccessfulShards(),
          response.getSkippedShards(),
          response.getTook().getMillis(),
          response.getShardFailures(),
          response.getClusters());
    }

    private List<InternalAggregation> toInternal(List<Aggregation> list) {
      List<InternalAggregation> t = new ArrayList<>(list.size());
      for (Aggregation a : list) {
        t.add((InternalAggregation) a);
      }
      return t;
    }

    protected DocumentGroup[] adapt(List<Cluster<InputDocument>> clusters, AtomicInteger groupId) {
      DocumentGroup[] groups = new DocumentGroup[clusters.size()];
      for (int i = 0; i < groups.length; i++) {
        groups[i] = adapt(clusters.get(i), groupId);
      }
      return groups;
    }

    private DocumentGroup adapt(Cluster<InputDocument> cluster, AtomicInteger groupId) {
      DocumentGroup group = new DocumentGroup();
      group.setId(groupId.incrementAndGet());
      group.setPhrases(cluster.getLabels().toArray(new String[0]));
      group.setScore(cluster.getScore());

      List<InputDocument> documents = cluster.getDocuments();
      String[] documentReferences = new String[documents.size()];
      for (int i = 0; i < documentReferences.length; i++) {
        documentReferences[i] = documents.get(i).getStringId();
      }
      group.setDocumentReferences(documentReferences);
      group.setSubgroups(adapt(cluster.getClusters(), groupId));

      return group;
    }

    /** Map {@link SearchHit} fields to logical fields of Carrot2 {@link Document}. */
    private List<InputDocument> prepareDocumentsForClustering(
        final ClusteringActionRequest request, SearchResponse response) {
      SearchHit[] hits = response.getHits().getHits();
      List<InputDocument> documents = new ArrayList<>(hits.length);
      List<FieldMappingSpec> fieldMapping = request.getFieldMapping();
      StringBuilder title = new StringBuilder();
      StringBuilder content = new StringBuilder();
      StringBuilder language = new StringBuilder();
      boolean emptySourceWarningEmitted = false;

      for (SearchHit hit : hits) {
        // Prepare logical fields for each hit.
        title.setLength(0);
        content.setLength(0);
        language.setLength(0);

        Map<String, DocumentField> fields = hit.getFields();
        Map<String, HighlightField> highlightFields = hit.getHighlightFields();

        Map<String, Object> sourceAsMap = null;
        for (FieldMappingSpec spec : fieldMapping) {
          // Determine the content source.
          Object appendContent = null;
          outer:
          switch (spec.source) {
            case FIELD:
              DocumentField hitField = fields.get(spec.field);
              if (hitField != null) {
                appendContent = hitField.getValue();
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
                    logger.warn(
                        "_source field mapping used but no source available for: {}, field {}",
                        hit.getId(),
                        spec.field);
                  }
                } else {
                  sourceAsMap = hit.getSourceAsMap();
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
                      logger.warn(
                          "Cannot find field named '{}' from spec: '{}'", fieldName, spec.field);
                      break outer;
                    }
                  } else {
                    logger.warn("Field is not a map: {} in spec.: {}", fieldName, spec.field);
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
            if (target.length() > 0) {
              target.append(" . ");
            }
            target.append(appendContent);
          }
        }

        String langCode = language.length() > 0 ? language.toString() : null;
        InputDocument doc =
            new InputDocument(title.toString(), content.toString(), langCode, hit.getId());

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

    private final class TransportHandler
        implements TransportRequestHandler<ClusteringActionRequest> {
      @Override
      public void messageReceived(
          final ClusteringActionRequest request, final TransportChannel channel, Task task)
          throws Exception {
        execute(
            request,
            new ActionListener<ClusteringActionResponse>() {
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
                  logger.warn(
                      "Failed to send error response for action ["
                          + ClusteringAction.NAME
                          + "] and request ["
                          + request
                          + "]",
                      e1);
                }
              }
            });
      }
    }
  }

  /** An {@link BaseRestHandler} for {@link ClusteringAction}. */
  public static class RestClusteringAction extends BaseRestHandler {
    protected Logger logger = LogManager.getLogger(getClass());

    /** Action name suffix. */
    public static String NAME = "_search_with_clusters";

    @Override
    public List<Route> routes() {
      return Arrays.asList(
          new Route(POST, "/" + NAME),
          new Route(POST, "/{index}/" + NAME),
          new Route(POST, "/{index}/{type}/" + NAME),
          new Route(GET, "/" + NAME),
          new Route(GET, "/{index}/" + NAME),
          new Route(GET, "/{index}/{type}/" + NAME));
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    @SuppressWarnings({"try", "deprecation"})
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
        throws IOException {
      // A POST request must have a body.
      if (request.method() == POST && !request.hasContent()) {
        return channel ->
            emitErrorResponse(
                channel,
                logger,
                new IllegalArgumentException("Request body was expected for a POST request."));
      }

      // Contrary to ES's default search handler we will not support
      // GET requests with a body (this is against HTTP spec guidance
      // in my opinion -- GET requests should not have a body).
      if (request.method() == GET && request.hasContent()) {
        return channel ->
            emitErrorResponse(
                channel,
                logger,
                new IllegalArgumentException("Request body was unexpected for a GET request."));
      }

      // Build an action request with data from the request.

      // Parse incoming arguments depending on the HTTP method used to make
      // the request.
      final ClusteringActionRequestBuilder actionBuilder =
          new ClusteringActionRequestBuilder(client);
      SearchRequest searchRequest = new SearchRequest();
      switch (request.method()) {
        case POST:
          searchRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
          searchRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
          actionBuilder.setSearchRequest(searchRequest);
          actionBuilder.setSource(
              request.content(), request.getXContentType(), request.getXContentRegistry());
          break;

        case GET:
          RestSearchAction.parseSearchRequest(
              searchRequest,
              request,
              null,
              (size) -> {
                searchRequest.source().size(size);
              });
          actionBuilder.setSearchRequest(searchRequest);
          fillFromGetRequest(actionBuilder, request);
          break;

        default:
          throw org.carrot2.elasticsearch.Preconditions.unreachable();
      }

      Set<String> passSecurityHeaders =
          new HashSet<>(Arrays.asList("es-security-runas-user", "_xpack_security_authentication"));

      Map<String, String> securityHeaders =
          client.threadPool().getThreadContext().getHeaders().entrySet().stream()
              .filter(e -> passSecurityHeaders.contains(e.getKey()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      // Dispatch clustering request.
      return channel -> {
        try (ThreadContext.StoredContext ignored =
            client.threadPool().getThreadContext().stashContext()) {
          client.threadPool().getThreadContext().copyHeaders(securityHeaders.entrySet());
          client.execute(
              ClusteringAction.INSTANCE,
              actionBuilder.request(),
              new ActionListener<ClusteringActionResponse>() {
                @Override
                public void onResponse(ClusteringActionResponse response) {
                  try {
                    XContentBuilder builder = channel.newBuilder();
                    builder.startObject();
                    response.toXContent(builder, request);
                    builder.endObject();
                    channel.sendResponse(
                        new BytesRestResponse(response.getSearchResponse().status(), builder));
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
      };
    }

    private static final EnumMap<LogicalField, String> GET_REQUEST_FIELDMAPPERS;

    static {
      GET_REQUEST_FIELDMAPPERS = new EnumMap<>(LogicalField.class);
      for (LogicalField lf : LogicalField.values()) {
        GET_REQUEST_FIELDMAPPERS.put(lf, "field_mapping_" + lf.name().toLowerCase(Locale.ROOT));
      }
    }

    /** Extract and parse HTTP GET parameters for the clustering request. */
    private void fillFromGetRequest(
        ClusteringActionRequestBuilder actionBuilder, RestRequest request) {
      // Use the search query as the query hint, if explicit query hint
      // is not available.
      if (request.hasParam(ClusteringActionRequest.JSON_QUERY_HINT)) {
        actionBuilder.setQueryHint(request.param(ClusteringActionRequest.JSON_QUERY_HINT));
      } else {
        actionBuilder.setQueryHint(request.param("q"));
      }

      if (request.hasParam(ClusteringActionRequest.JSON_ALGORITHM)) {
        actionBuilder.setAlgorithm(request.param(ClusteringActionRequest.JSON_ALGORITHM));
      }

      if (request.hasParam(ClusteringActionRequest.JSON_MAX_HITS)) {
        actionBuilder.setMaxHits(request.param(ClusteringActionRequest.JSON_MAX_HITS));
      }

      if (request.hasParam(ClusteringActionRequest.JSON_CREATE_UNGROUPED_CLUSTER)) {
        actionBuilder.setCreateUngroupedDocumentsCluster(
            Boolean.parseBoolean(
                request.param(ClusteringActionRequest.JSON_CREATE_UNGROUPED_CLUSTER)));
      }

      if (request.hasParam(ClusteringActionRequest.JSON_LANGUAGE)) {
        actionBuilder.setDefaultLanguage(request.param(ClusteringActionRequest.JSON_LANGUAGE));
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
