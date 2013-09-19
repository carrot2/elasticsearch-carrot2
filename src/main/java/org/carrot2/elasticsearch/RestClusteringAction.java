package org.carrot2.elasticsearch;

import static org.carrot2.elasticsearch.LoggerUtils.emitErrorResponse;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.restContentBuilder;

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

/**
 */
public class RestClusteringAction extends BaseRestHandler {
    /**
     * Action name suffix.
     */
    public static String NAME = "_search_with_clusters";

    @Inject
    public RestClusteringAction(
            Settings settings, 
            Client client, 
            RestController controller) {
        super(settings, client);

        controller.registerHandler(POST, "/" + NAME,                this);
        controller.registerHandler(POST, "/{index}/" + NAME,        this);
        controller.registerHandler(POST, "/{index}/{type}/" + NAME, this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel) {
        if (!request.hasContent()) {
            emitErrorResponse(channel, request, logger, new RuntimeException("Request body was expected."));
            return;
        }

        // Fill action request with data from REST request.
        // TODO: delegate json parsing to Carrot2ClusteringAction.
        ClusteringActionRequest actionRequest = new ClusteringActionRequest();
        fillFromSource(request, actionRequest, request.content());

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
                    logger.debug("Failed to emit response.", e);
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                emitErrorResponse(channel, request, logger, e);
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
