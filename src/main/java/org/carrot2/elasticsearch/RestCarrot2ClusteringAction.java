package org.carrot2.elasticsearch;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
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

    @Inject
    public RestCarrot2ClusteringAction(
            Settings settings, 
            Client client, 
            RestController controller,
            final RestSearchAction restSearchAction) {
        super(settings, client);

        controller.registerHandler(POST, "/_search_with_clusters",                this);
        controller.registerHandler(POST, "/{index}/_search_with_clusters",        this);
        controller.registerHandler(POST, "/{index}/{type}/_search_with_clusters", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        // TODO: delegate json parsing to Carrot2ClusteringAction.
        ClusteringActionRequest actionRequest = new ClusteringActionRequest();
        if (request.hasContent()) {
            fillFromSource(request, actionRequest, request.content());
        } else {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, new RuntimeException("Request body was expected.")));
            } catch (IOException e) {
                logger.error("Failed to send failure response", e);
            }
        }

        // Build a clustering request and dispatch.
        client.execute(ClusteringAction.INSTANCE, actionRequest, 
            new ActionListener<ClusteringActionResponse>() {
            @Override
            public void onResponse(ClusteringActionResponse response) {
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

    /**
     * Parse the clustering/ search request JSON.
     */
    @SuppressWarnings("unchecked")
    private void fillFromSource(
            RestRequest restRequest, 
            ClusteringActionRequest clusteringRequest,
            BytesReference source) {
        if (source == null || source.length() == 0) {
            return;
        }

        XContentParser parser = null;
        try {
            // TODO: we should avoid reparsing here but it's terribly difficult to slice
            // the underlying byte buffer to get just the search request.

            parser = XContentFactory.xContent(source).createParser(source);
            Map<String, Object> asMap = parser.mapOrderedAndClose();

            if (asMap.get("query_hint") != null) {
                clusteringRequest.setQueryHint((String) asMap.get("query_hint"));
            }
            if (asMap.containsKey("field_mapping")) {
                parseFieldSpecs(clusteringRequest, (Map<String,List<String>>) asMap.get("field_mapping"));
            }
            if (asMap.containsKey("search_request")) {
                SearchRequest searchRequest = new SearchRequest();
                searchRequest.indices(Strings.splitStringByCommaToArray(restRequest.param("index")));
                searchRequest.source((Map<?,?>) asMap.get("search_request"));
                searchRequest.types(Strings.splitStringByCommaToArray(restRequest.param("type")));
                clusteringRequest.setSearchRequest(searchRequest);
            }
            if (asMap.containsKey("algorithm")) {
                clusteringRequest.setAlgorithm((String) asMap.get("algorithm"));
            }
            if (asMap.containsKey("attributes")) {
                clusteringRequest.setAttributes((Map<String,Object>) asMap.get("attributes"));
            }
        } catch (Exception e) {
            String sSource = "_na_";
            try {
                sSource = XContentHelper.convertToJson(source, false);
            } catch (Throwable e1) {
                // ignore
            }
            throw new ElasticSearchException("Failed to parse source [" + sSource + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }        
    }

    private void parseFieldSpecs(ClusteringActionRequest actionRequest,
            Map<String, List<String>> fieldSpecs) {
        for (Map.Entry<String,List<String>> e : fieldSpecs.entrySet()) {
            LogicalField logicalField = LogicalField.valueOfCaseInsensitive(e.getKey());
            if (logicalField != null) {
                for (String fieldSpec : e.getValue()) {
                    actionRequest.addFieldMappingSpec(fieldSpec, logicalField);
                }
            }
        }
    }
}
