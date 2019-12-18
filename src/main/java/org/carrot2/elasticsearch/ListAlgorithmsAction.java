package org.carrot2.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
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
public class ListAlgorithmsAction extends ActionType<ListAlgorithmsAction.ListAlgorithmsActionResponse> {
    /* Action name. */
    public static final String NAME = "cluster:monitor/carrot2/algorithms";

    /* Reusable singleton. */
    public static final ListAlgorithmsAction INSTANCE = new ListAlgorithmsAction();

    private ListAlgorithmsAction() {
        super(NAME, ListAlgorithmsActionResponse::new);
    }

    @Override
    public Writeable.Reader<ListAlgorithmsActionResponse> getResponseReader() {
        return ListAlgorithmsActionResponse::new;
    }

    /**
     * An {@link ActionRequest} for {@link ListAlgorithmsAction}.
     */
    public static class ListAlgorithmsActionRequest
            extends ActionRequest {

        ListAlgorithmsActionRequest() {}

        ListAlgorithmsActionRequest(StreamInput in) throws IOException {
            super(in);
        }

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
            ListAlgorithmsActionResponse> {
        public ListAlgorithmsActionRequestBuilder(ElasticsearchClient client) {
            super(client, ListAlgorithmsAction.INSTANCE, new ListAlgorithmsActionRequest());
        }
    }

    /**
     * A {@link ActionResponse} for {@link ListAlgorithmsAction}.
     */
    public static class ListAlgorithmsActionResponse extends ActionResponse implements ToXContent {
        private static final String[] EMPTY_LIST = {};
        private String [] algorithms;

        /**
         * Clustering-related response fields.
         */
        static final class Fields {
            static final String ALGORITHMS = "algorithms";
        }

        public ListAlgorithmsActionResponse(StreamInput in) throws IOException {
            super(in);
            algorithms = in.readStringArray();
        }

        public ListAlgorithmsActionResponse(List<String> algorithms) {
            this.algorithms = algorithms.toArray(new String[0]);
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
            out.writeStringArray(algorithms);
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
            extends TransportAction<ListAlgorithmsActionRequest, ListAlgorithmsActionResponse> {

        protected Logger logger = LogManager.getLogger(getClass());
        private final ControllerSingleton controllerSingleton;

        @Inject
        public TransportListAlgorithmsAction(TransportService transportService,
                                             ControllerSingleton controllerSingleton,
                                             ActionFilters actionFilters) {
            super(ListAlgorithmsAction.NAME,
                  actionFilters,
                  transportService.getTaskManager());
            this.controllerSingleton = controllerSingleton;
            transportService.registerRequestHandler(
                    ListAlgorithmsAction.NAME,
                    ThreadPool.Names.SAME,
                    ListAlgorithmsActionRequest::new,
                    new TransportHandler());
        }

        @Override
        protected void doExecute(Task task,
                                 ListAlgorithmsActionRequest request,
                                 ActionListener<ListAlgorithmsActionResponse> listener) {
            listener.onResponse(new ListAlgorithmsActionResponse(controllerSingleton.getAlgorithms()));
        }

        private final class TransportHandler implements TransportRequestHandler<ListAlgorithmsActionRequest> {
            @Override
            public void messageReceived(final ListAlgorithmsActionRequest request,
                                        final TransportChannel channel,
                                        Task task) throws Exception {
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

        protected Logger logger = LogManager.getLogger(getClass());

        public RestListAlgorithmsAction(
                RestController controller) {

            controller.registerHandler(Method.POST, "/" + NAME, this);
            controller.registerHandler(Method.GET, "/" + NAME, this);
        }

        @Override
        public String getName() {
            return NAME;
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
