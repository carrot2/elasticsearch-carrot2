package org.carrot2.elasticsearch;

import static org.carrot2.elasticsearch.LoggerUtils.emitErrorResponse;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;

/** Perform clustering of search results. */
public class ClusteringAction extends ActionType<ClusteringActionResponse> {
  /* Action name. */
  public static final String NAME = "indices:data/read/cluster";

  /* Reusable singleton. */
  public static final ClusteringAction INSTANCE = new ClusteringAction();

  private ClusteringAction() {
    super(NAME, ClusteringActionResponse::new);
  }

  @Override
  public Writeable.Reader<ClusteringActionResponse> getResponseReader() {
    return ClusteringActionResponse::new;
  }

  /** An {@link BaseRestHandler} for {@link ClusteringAction}. */
  public static class RestClusteringAction extends BaseRestHandler {
    protected Logger logger = LogManager.getLogger(getClass());

    /** Action name suffix. */
    public static String NAME = "_search_with_clusters";

    @Override
    public List<Route> routes() {
      return Arrays.asList(
          new Route(POST, "/" + NAME),
          new Route(POST, "/{index}/" + NAME),
          new Route(POST, "/{index}/{type}/" + NAME),
          new Route(GET, "/" + NAME),
          new Route(GET, "/{index}/" + NAME),
          new Route(GET, "/{index}/{type}/" + NAME));
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    @SuppressWarnings({"try", "deprecation"})
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client)
        throws IOException {
      // A POST request must have a body.
      if (request.method() == POST && !request.hasContent()) {
        return channel ->
            emitErrorResponse(
                channel,
                logger,
                new IllegalArgumentException("Request body was expected for a POST request."));
      }

      // Contrary to ES's default search handler we will not support
      // GET requests with a body (this is against HTTP spec guidance
      // in my opinion -- GET requests should not have a body).
      if (request.method() == GET && request.hasContent()) {
        return channel ->
            emitErrorResponse(
                channel,
                logger,
                new IllegalArgumentException("Request body was unexpected for a GET request."));
      }

      // Build an action request with data from the request.

      // Parse incoming arguments depending on the HTTP method used to make
      // the request.
      final ClusteringActionRequestBuilder actionBuilder =
          new ClusteringActionRequestBuilder(client);
      SearchRequest searchRequest = new SearchRequest();
      switch (request.method()) {
        case POST:
          searchRequest.indices(Strings.splitStringByCommaToArray(request.param("index")));
          searchRequest.types(Strings.splitStringByCommaToArray(request.param("type")));
          actionBuilder.setSearchRequest(searchRequest);
          actionBuilder.setSource(
              request.content(), request.getXContentType(), request.getXContentRegistry());
          break;

        case GET:
          RestSearchAction.parseSearchRequest(
              searchRequest,
              request,
              null,
              (size) -> {
                searchRequest.source().size(size);
              });
          actionBuilder.setSearchRequest(searchRequest);
          fillFromGetRequest(actionBuilder, request);
          break;

        default:
          throw org.carrot2.elasticsearch.Preconditions.unreachable();
      }

      Set<String> passSecurityHeaders =
          new HashSet<>(Arrays.asList("es-security-runas-user", "_xpack_security_authentication"));

      Map<String, String> securityHeaders =
          client.threadPool().getThreadContext().getHeaders().entrySet().stream()
              .filter(e -> passSecurityHeaders.contains(e.getKey()))
              .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

      // Dispatch clustering request.
      return channel -> {
        try (ThreadContext.StoredContext ignored =
            client.threadPool().getThreadContext().stashContext()) {
          client.threadPool().getThreadContext().copyHeaders(securityHeaders.entrySet());
          client.execute(
              ClusteringAction.INSTANCE,
              actionBuilder.request(),
              new ActionListener<ClusteringActionResponse>() {
                @Override
                public void onResponse(ClusteringActionResponse response) {
                  try {
                    XContentBuilder builder = channel.newBuilder();
                    builder.startObject();
                    response.toXContent(builder, request);
                    builder.endObject();
                    channel.sendResponse(
                        new BytesRestResponse(response.getSearchResponse().status(), builder));
                  } catch (Exception e) {
                    logger.debug("Failed to emit response.", e);
                    onFailure(e);
                  }
                }

                @Override
                public void onFailure(Exception e) {
                  emitErrorResponse(channel, logger, e);
                }
              });
        }
      };
    }

    private static final EnumMap<LogicalField, String> GET_REQUEST_FIELDMAPPERS;

    static {
      GET_REQUEST_FIELDMAPPERS = new EnumMap<>(LogicalField.class);
      for (LogicalField lf : LogicalField.values()) {
        GET_REQUEST_FIELDMAPPERS.put(lf, "field_mapping_" + lf.name().toLowerCase(Locale.ROOT));
      }
    }

    /** Extract and parse HTTP GET parameters for the clustering request. */
    private void fillFromGetRequest(
        ClusteringActionRequestBuilder actionBuilder, RestRequest request) {
      // Use the search query as the query hint, if explicit query hint
      // is not available.
      if (request.hasParam(ClusteringActionRequest.JSON_QUERY_HINT)) {
        actionBuilder.setQueryHint(request.param(ClusteringActionRequest.JSON_QUERY_HINT));
      } else {
        actionBuilder.setQueryHint(request.param("q"));
      }

      if (request.hasParam(ClusteringActionRequest.JSON_ALGORITHM)) {
        actionBuilder.setAlgorithm(request.param(ClusteringActionRequest.JSON_ALGORITHM));
      }

      if (request.hasParam(ClusteringActionRequest.JSON_MAX_HITS)) {
        actionBuilder.setMaxHits(request.param(ClusteringActionRequest.JSON_MAX_HITS));
      }

      if (request.hasParam(ClusteringActionRequest.JSON_CREATE_UNGROUPED_CLUSTER)) {
        actionBuilder.setCreateUngroupedDocumentsCluster(
            Boolean.parseBoolean(
                request.param(ClusteringActionRequest.JSON_CREATE_UNGROUPED_CLUSTER)));
      }

      if (request.hasParam(ClusteringActionRequest.JSON_LANGUAGE)) {
        actionBuilder.setDefaultLanguage(request.param(ClusteringActionRequest.JSON_LANGUAGE));
      }

      // Field mappers.
      for (Map.Entry<LogicalField, String> e : GET_REQUEST_FIELDMAPPERS.entrySet()) {
        if (request.hasParam(e.getValue())) {
          for (String spec : Strings.splitStringByCommaToArray(request.param(e.getValue()))) {
            actionBuilder.addFieldMappingSpec(spec, e.getKey());
          }
        }
      }
    }
  }
}
