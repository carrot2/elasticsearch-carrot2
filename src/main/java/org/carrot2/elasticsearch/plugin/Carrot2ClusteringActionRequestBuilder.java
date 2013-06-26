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

    @Override
    protected void doExecute(
            ActionListener<Carrot2ClusteringActionResponse> listener) {
        ((Client) client).execute(Carrot2ClusteringAction.INSTANCE, request, listener);
    }
}
