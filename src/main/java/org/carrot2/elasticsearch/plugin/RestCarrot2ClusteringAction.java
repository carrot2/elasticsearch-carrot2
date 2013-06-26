package org.carrot2.elasticsearch.plugin;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.search.RestSearchAction;

/**
 */
public class RestCarrot2ClusteringAction extends BaseRestHandler {
    /**
     * Action name suffix.
     */
    public static String NAME = "_search_with_clusters";
    
    /**
     * {@link RestSearchAction} parsing to {@link SearchRequest} encapsulation. 
     */
    private final Function<RestRequest, SearchRequest> searchRequestParser;

    @Inject
    public RestCarrot2ClusteringAction(
            Settings settings, 
            Client client, 
            RestController controller,
            final RestSearchAction restSearchAction) {
        super(settings, client);

        /*
         * We don't want to copy-paste and replicate the complexity of search request
         * parsing present in {@link RestSearchAction} so we delegate this parsing
         * via reflection. If something goes wrong (security manager, an API change, 
         * or something else) we just bail out.
         */
        try {
            final Method parseSearchRequestMethod = restSearchAction.getClass().getDeclaredMethod(
                    "parseSearchRequest",
                    RestRequest.class);
            parseSearchRequestMethod.setAccessible(true);
            searchRequestParser = new Function<RestRequest, SearchRequest>() {
                @Override
                public SearchRequest apply(RestRequest request) {
                    try {
                        return (SearchRequest) parseSearchRequestMethod.invoke(restSearchAction, request);
                    } catch (IllegalArgumentException e) {
                        throw new RuntimeException(e);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    } catch (InvocationTargetException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        } catch (Exception e) {
            throw new ElasticSearchException(
                    "Could not initialize delegation of search request parsing to " + 
                    restSearchAction.getClass().getName() + "#parseSearchRequest", e);
        }

        controller.registerHandler( GET, "/_search_with_clusters",                this);
        controller.registerHandler(POST, "/_search_with_clusters",                this);
        controller.registerHandler( GET, "/{index}/_search_with_clusters",        this);
        controller.registerHandler(POST, "/{index}/_search_with_clusters",        this);
        controller.registerHandler( GET, "/{index}/{type}/_search_with_clusters", this);
        controller.registerHandler(POST, "/{index}/{type}/_search_with_clusters", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        SearchRequest searchRequest = searchRequestParser.apply(request);

        // TODO: build a C2clustering request and dispatch.
        new Carrot2ClusteringRequestBuilder(client)
                .setSearchRequest(searchRequest)
                .execute(new ActionListener<Carrot2ClusteringActionResponse>() {
                    @Override
                    public void onResponse(Carrot2ClusteringActionResponse response) {
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
                            if (logger.isDebugEnabled()) {
                                logger.debug("failed to execute search (building response)", e);
                            }
                            onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        try {
                            channel.sendResponse(new XContentThrowableRestResponse(request, e));
                        } catch (IOException e1) {
                            logger.error("Failed to send failure response", e1);
                        }
                    }
                });
    }
}
