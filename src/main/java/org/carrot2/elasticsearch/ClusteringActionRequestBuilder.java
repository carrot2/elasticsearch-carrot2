
package org.carrot2.elasticsearch;

import java.util.HashMap;
import java.util.Map;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;

/** An {@link ActionRequestBuilder} for {@link ClusteringAction}. */
public class ClusteringActionRequestBuilder
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
