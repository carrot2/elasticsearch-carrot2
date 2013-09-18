package org.carrot2.elasticsearch;

import org.elasticsearch.action.Action;
import org.elasticsearch.client.Client;

public class ClusteringAction 
    extends Action<ClusteringActionRequest, 
                   ClusteringActionResponse, 
                   ClusteringActionRequestBuilder> {
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
}
