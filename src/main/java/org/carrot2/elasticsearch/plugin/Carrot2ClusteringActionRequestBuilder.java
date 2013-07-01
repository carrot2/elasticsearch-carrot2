package org.carrot2.elasticsearch.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.internal.InternalClient;

/* */
public class Carrot2ClusteringActionRequestBuilder 
    extends ActionRequestBuilder<Carrot2ClusteringActionRequest, 
                                 Carrot2ClusteringActionResponse, 
                                 Carrot2ClusteringActionRequestBuilder> {

    public Carrot2ClusteringActionRequestBuilder(Client client) {
        super((InternalClient) client, new Carrot2ClusteringActionRequest());
    }

    public Carrot2ClusteringActionRequestBuilder setSearchRequest(SearchRequestBuilder builder) {
        super.request.setSearchRequest(builder);
        return this;
    }

    public Carrot2ClusteringActionRequestBuilder setSearchRequest(SearchRequest searchRequest) {
        super.request.setSearchRequest(searchRequest);
        return this;
    }
    
    public Carrot2ClusteringActionRequestBuilder setQueryHint(String queryHint) {
        if (queryHint == null) {
            throw new IllegalArgumentException("Query hint may be empty but must not be null.");
        }
        super.request.setQueryHint(queryHint);
        return this;
    }

    public Carrot2ClusteringActionRequestBuilder setAlgorithm(String algorithm) {
        super.request.setAlgorithm(algorithm);
        return this;
    }

    public Carrot2ClusteringActionRequestBuilder addFieldMapping(String fieldName, LogicalField logicalField) {
        super.request.addFieldMapping(fieldName, logicalField);
        return this;
    }

    public Carrot2ClusteringActionRequestBuilder addSourceFieldMapping(String fieldName, LogicalField logicalField) {
        super.request.addSourceFieldMapping(fieldName, logicalField);
        return this;
    }

    public Carrot2ClusteringActionRequestBuilder addHighlightedFieldMapping(String fieldName, LogicalField logicalField) {
        super.request.addHighlightedFieldMapping(fieldName, logicalField);
        return this;
    }

    public Carrot2ClusteringActionRequestBuilder addFieldMappingSpec(String fieldSpec, LogicalField logicalField) {
        super.request.addFieldMappingSpec(fieldSpec, logicalField);
        return this;
    }

    @Override
    protected void doExecute(
            ActionListener<Carrot2ClusteringActionResponse> listener) {
        ((Client) client).execute(Carrot2ClusteringAction.INSTANCE, request, listener);
    }
}
