package org.carrot2.elasticsearch;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.carrot2.elasticsearch.LoggerUtils.emitErrorResponse;

/**
 * List all available clustering algorithms.
 */
public class ListAlgorithmsAction extends Action<ListAlgorithmsAction.ListAlgorithmsActionRequest,
        ListAlgorithmsAction.ListAlgorithmsActionResponse,
        ListAlgorithmsAction.ListAlgorithmsActionRequestBuilder> {
    /* Action name. */
    public static final String NAME = "clustering/list";

    /* Reusable singleton. */
    public static final ListAlgorithmsAction INSTANCE = new ListAlgorithmsAction();

    private ListAlgorithmsAction() {
        super(NAME);
    }

    @Override
    public ListAlgorithmsActionResponse newResponse() {
        return new ListAlgorithmsActionResponse();
    }

    @Override
    public ListAlgorithmsActionRequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new ListAlgorithmsActionRequestBuilder(client);
    }

    /**
     * An {@link ActionRequest} for {@link ListAlgorithmsAction}.
     */
    public static class ListAlgorithmsActionRequest
            extends ActionRequest {
        @Override
        public ActionRequestValidationException validate() {
            return /* Nothing to validate. */ null;
        }
    }

    /**
     * An {@link ActionRequestBuilder} for {@link ListAlgorithmsAction}.
     */
    public static class ListAlgorithmsActionRequestBuilder
            extends ActionRequestBuilder<ListAlgorithmsActionRequest,
            ListAlgorithmsActionResponse,
            ListAlgorithmsActionRequestBuilder> {
        public ListAlgorithmsActionRequestBuilder(ElasticsearchClient client) {
            super(client, ListAlgorithmsAction.INSTANCE, new ListAlgorithmsActionRequest());
        }
    }

    /**
     * A {@link ActionResponse} for {@link ListAlgorithmsAction}.
     */
    public static class ListAlgorithmsActionResponse extends ActionResponse implements ToXContent {
        private static final String[] EMPTY_LIST = {};
        private String[] algorithms;

        /**
         * Clustering-related response fields.
         */
        static final class Fields {
            static final String ALGORITHMS = "algorithms";
        }

        private ListAlgorithmsActionResponse(String[] algorithms) {
            this.algorithms = algorithms;
        }

        public ListAlgorithmsActionResponse() {
            this(EMPTY_LIST);
        }

        public ListAlgorithmsActionResponse(List<String> algorithms) {
            this(algorithms.toArray(new String[algorithms.size()]));
        }

        public List<String> getAlgorithms() {
            return Collections.unmodifiableList(Arrays.asList(algorithms));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params)
                throws IOException {
            return builder.array(Fields.ALGORITHMS, algorithms);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(algorithms);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            algorithms = in.readStringArray();
        }

        @Override
        public String toString() {
            return ToString.objectToJson(this);
        }
    }

    /**
     * A {@link TransportAction} for actually executing
     * {@link ListAlgorithmsActionRequest} and providing
     * {@link ListAlgorithmsActionResponse}.
     */
    public static class TransportListAlgorithmsAction
            extends TransportAction<ListAlgorithmsActionRequest,
            ListAlgorithmsActionResponse> {

        private final ControllerSingleton controllerSingleton;

        @Inject
        public TransportListAlgorithmsAction(Settings settings, ThreadPool threadPool,
                                             TransportService transportService,
                                             ControllerSingleton controllerSingleton,
                                             ActionFilters actionFilters,
                                             IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings,
                  ListAlgorithmsAction.NAME,
                  threadPool,
                  actionFilters,
                  indexNameExpressionResolver,
                  transportService.getTaskManager());
            this.controllerSingleton = controllerSingleton;
            transportService.registerRequestHandler(
                    ListAlgorithmsAction.NAME,
                    ListAlgorithmsActionRequest::new,
                    ThreadPool.Names.SAME,
                    new TransportHandler());
        }

        @Override
        protected void doExecute(ListAlgorithmsActionRequest request,
                                 ActionListener<ListAlgorithmsActionResponse> listener) {
            listener.onResponse(new ListAlgorithmsActionResponse(controllerSingleton.getAlgorithms()));
        }

        private final class TransportHandler implements TransportRequestHandler<ListAlgorithmsActionRequest> {
            @Override
            public void messageReceived(final ListAlgorithmsActionRequest request,
                                        final TransportChannel channel) throws Exception {
                execute(request, new ActionListener<ListAlgorithmsActionResponse>() {
                    @Override
                    public void onResponse(ListAlgorithmsActionResponse response) {
                        try {
                            channel.sendResponse(response);
                        } catch (Exception e) {
                            onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            channel.sendResponse(e);
                        } catch (Exception e1) {
                            logger.warn("Failed to send error response for action ["
                                    + NAME + "] and request [" + request + "]", e1);
                        }
                    }
                });
            }
        }
    }

    /**
     * {@link BaseRestHandler} for serving {@link ListAlgorithmsAction}.
     */
    public static class RestListAlgorithmsAction extends BaseRestHandler {
        /* Action name suffix. */
        public static String NAME = "_algorithms";

        @Inject
        public RestListAlgorithmsAction(
                Settings settings,
                RestController controller) {
            super(settings);

            controller.registerHandler(Method.POST, "/" + NAME, this);
            controller.registerHandler(Method.GET, "/" + NAME, this);
        }

        @Override
        public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
            if (request.hasContent()) {
                return channel -> emitErrorResponse(channel, logger,
                        new IllegalArgumentException("Request body was expected."));
            }

            ListAlgorithmsActionRequest actionRequest = new ListAlgorithmsActionRequest();
            return channel -> client.execute(INSTANCE, actionRequest, new ActionListener<ListAlgorithmsActionResponse>() {
                @Override
                public void onResponse(ListAlgorithmsActionResponse response) {
                    try {
                        XContentBuilder builder = channel.newBuilder();
                        builder.startObject();
                        response.toXContent(builder, request);
                        builder.endObject();
                        channel.sendResponse(
                                new BytesRestResponse(
                                        RestStatus.OK,
                                        builder));
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
    }
}
