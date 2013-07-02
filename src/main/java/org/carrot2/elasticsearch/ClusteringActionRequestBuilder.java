package org.carrot2.elasticsearch;

import java.util.Map;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;
import org.elasticsearch.common.collect.ImmutableMap;

import com.google.common.collect.Maps;

/** */
public class ClusteringActionRequestBuilder 
    extends ActionRequestBuilder<ClusteringActionRequest, 
                                 ClusteringActionResponse, 
                                 ClusteringActionRequestBuilder> {

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
