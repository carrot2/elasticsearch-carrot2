package org.carrot2.elasticsearch.plugin;

import org.carrot2.core.Controller;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

public class TransportCarrot2ClusteringAction  
    extends TransportAction<Carrot2ClusteringActionRequest,
                            Carrot2ClusteringActionResponse> {

    private final TransportSearchAction searchAction;
    private final ControllerSingleton controllerSingleton;

    @Inject
    protected TransportCarrot2ClusteringAction(Settings settings, ThreadPool threadPool,
            TransportService transportService,
            TransportSearchAction searchAction,
            ControllerSingleton controllerSingleton) {
        super(settings, threadPool);
        this.searchAction = searchAction;
        this.controllerSingleton = controllerSingleton;
        transportService.registerHandler(Carrot2ClusteringAction.NAME, new TransportHandler());
    }

    @Override
    protected void doExecute(Carrot2ClusteringActionRequest request,
                             final ActionListener<Carrot2ClusteringActionResponse> listener) {
        searchAction.execute(request.getSearchRequest(), new ActionListener<SearchResponse>() {
            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }

            @Override
            public void onResponse(SearchResponse response) {
                Controller controller = controllerSingleton.getController();

                // TODO: pick fields, cluster.
                controller.getStatistics();

                listener.onResponse(new Carrot2ClusteringActionResponse(response));
            }
        });
    }

    private final class TransportHandler extends BaseTransportRequestHandler<Carrot2ClusteringActionRequest> {
        @Override
        public Carrot2ClusteringActionRequest newInstance() {
            return new Carrot2ClusteringActionRequest();
        }

        @Override
        public void messageReceived(final Carrot2ClusteringActionRequest request, final TransportChannel channel) throws Exception {
            request.listenerThreaded(false);
            execute(request, new ActionListener<Carrot2ClusteringActionResponse>() {
                @Override
                public void onResponse(Carrot2ClusteringActionResponse response) {
                    try {
                        channel.sendResponse(response);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception e1) {
                        logger.warn("Failed to send error response for action ["
                                + Carrot2ClusteringAction.NAME + "] and request [" + request + "]", e1);
                    }
                }
            });
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}
