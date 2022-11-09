package org.carrot2.elasticsearch;

import static org.elasticsearch.action.ValidateActions.addValidationError;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

/** An {@link ActionRequest} for {@link ClusteringAction}. */
public class ClusteringActionRequest extends ActionRequest implements IndicesRequest.Replaceable {
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
  boolean createUngroupedDocumentsCluster;
  private String defaultLanguage = "English";

  /**
   * Set the {@link SearchRequest} to use for fetching documents to be clustered. The search request
   * must fetch enough documents for clustering to make sense (set <code>size</code> appropriately).
   *
   * @param searchRequest search request to set
   * @return same builder instance
   */
  public ClusteringActionRequest setSearchRequest(SearchRequest searchRequest) {
    this.searchRequest = searchRequest;
    return this;
  }

  /**
   * @param builder The search builder
   * @return Returns same object for chaining.
   * @see #setSearchRequest(SearchRequest)
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
   *     algorithm to avoid trivial clusters around the query terms. Typically the query terms hint
   *     will be identical to what the user typed in the search box.
   *     <p>The hint may be an empty string but must not be <code>null</code>.
   * @return same builder instance
   */
  public ClusteringActionRequest setQueryHint(String queryHint) {
    this.queryHint = queryHint;
    return this;
  }

  /**
   * @return Query hint
   * @see #setQueryHint(String)
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
   * @return The current algorithm to use for clustering
   * @see #setAlgorithm
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
   * @return Clustering algorithm attributes map
   * @see #setAttributes(Map)
   */
  public Map<String, Object> getAttributes() {
    return attributes;
  }

  /**
   * Parses some {@link org.elasticsearch.xcontent.XContent} and fills in the request.
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
        XContentParserConfiguration parserConfig =
            XContentParserConfiguration.EMPTY
                .withRegistry(xContentRegistry)
                .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        XContentParser searchXParser =
            XContentFactory.xContent(XContentType.JSON)
                .createParser(parserConfig, Strings.toString(builder));
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
   * @param fieldName field name
   * @param logicalField logical field mapping.
   * @return Same object for chaining
   * @see LogicalField
   */
  public ClusteringActionRequest addFieldMapping(String fieldName, LogicalField logicalField) {
    fieldMapping.add(new FieldMappingSpec(fieldName, logicalField, FieldSource.FIELD));
    return this;
  }

  /**
   * Map a hit's source field (field unpacked from the <code>_source</code> document) to a logical
   * section of a document to be clustered (title, content or URL).
   *
   * @param sourceFieldName field name
   * @param logicalField logical field mapping.
   * @return Same object for chaining
   * @see LogicalField
   */
  public ClusteringActionRequest addSourceFieldMapping(
      String sourceFieldName, LogicalField logicalField) {
    fieldMapping.add(new FieldMappingSpec(sourceFieldName, logicalField, FieldSource.SOURCE));
    return this;
  }

  /**
   * Map a hit's highligted field (fragments of the original field) to a logical section of a
   * document to be clustered. This may be used to decrease the amount of information passed to the
   * clustering engine but also to "focus" the clustering engine on the context of the query.
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
   * @param fieldSpec field specification
   * @param logicalField logical field mapping.
   * @return Same object for chaining
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
          addValidationError("query hint may be empty but must not be null.", validationException);
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
      out.writeMap(attributes, StreamOutput::writeString, StreamOutput::writeGenericValue);
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
