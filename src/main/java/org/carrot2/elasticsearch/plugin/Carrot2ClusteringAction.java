package org.carrot2.elasticsearch.plugin;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

public class Carrot2ClusteringAction 
    extends Action<Carrot2ClusteringActionRequest, 
                   Carrot2ClusteringActionResponse, 
                   Carrot2ClusteringRequestBuilder> {

    public static final String NAME = "misc/carrot2";
    public static final Carrot2ClusteringAction INSTANCE = new Carrot2ClusteringAction();

    private Carrot2ClusteringAction() {
        super(NAME);
    }

    @Override
    public Carrot2ClusteringRequestBuilder newRequestBuilder(Client client) {
        return new Carrot2ClusteringRequestBuilder(client);
    }

    @Override
    public Carrot2ClusteringActionResponse newResponse() {
        return new Carrot2ClusteringActionResponse();
    }
}
