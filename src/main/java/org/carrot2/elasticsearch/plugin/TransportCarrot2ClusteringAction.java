package org.carrot2.elasticsearch.plugin;

import java.util.List;
import java.util.Map;

import org.carrot2.clustering.lingo.LingoClusteringAlgorithm;
import org.carrot2.core.Controller;
import org.carrot2.core.Document;
import org.carrot2.core.ProcessingResult;
import org.carrot2.core.attribute.CommonAttributesDescriptor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportRequestHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;
import org.testng.collections.Maps;

import com.google.common.collect.Lists;

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
    protected void doExecute(final Carrot2ClusteringActionRequest request,
                             final ActionListener<Carrot2ClusteringActionResponse> listener) {
        searchAction.execute(request.getSearchRequest(), new ActionListener<SearchResponse>() {
            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }

            @Override
            public void onResponse(SearchResponse response) {
                /*
                 * This is where the main clustering "logic" takes place.
                 * TODO: is this the right place (thread)?
                 */
                Controller controller = controllerSingleton.getController();

                Map<String,Object> processingAttrs = Maps.newHashMap();
                CommonAttributesDescriptor.attributeBuilder(processingAttrs)
                    .documents(prepareDocumentsForClustering(request, response))
                    .query(request.getQueryHint());

                // TODO: algorithm picking, initialization options?
                ProcessingResult result = 
                        controller.process(processingAttrs, LingoClusteringAlgorithm.class);

                listener.onResponse(new Carrot2ClusteringActionResponse(response, result.getClusters()));
            }
        });
    }

    /**
     * Map {@link SearchHit} fields to logical fields of Carrot2 {@link Document}.
     */
    private List<Document> prepareDocumentsForClustering(
            final Carrot2ClusteringActionRequest request,
            SearchResponse response) {
        SearchHit [] hits = response.getHits().hits();
        List<Document> documents = Lists.newArrayListWithCapacity(hits.length);
        List<FieldMappingSpec> fieldMapping = request.getFieldMapping();
        StringBuilder title = new StringBuilder();
        StringBuilder content = new StringBuilder();
        StringBuilder url = new StringBuilder();

        for (SearchHit hit : hits) {
            // Prepare logical fields for each hit.
            title.setLength(0);
            content.setLength(0);
            url.setLength(0);
            
            Map<String, SearchHitField> fields = hit.getFields();
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();

            for (FieldMappingSpec spec : fieldMapping) {
                // Determine the content source.
                Object appendContent;
                if (spec.source == FieldSource.FIELD) {
                    appendContent = fields.get(spec.field);
                } else {
                    appendContent = highlightFields.get(spec.field);
                }

                // Determine the target field.
                if (appendContent != null) {
                    StringBuilder target;
                    switch (spec.logicalField) {
                        case URL:
                            url.setLength(0); // Clear previous (single mapping allowed).
                            target = url;
                            break;
                        case TITLE:
                            target = title;
                            break;
                        case CONTENT:
                            target = content;
                            break;
                        default:
                            throw new RuntimeException("Unreachable.");
                    }

                    // Separate multiple fields with a single dot (prevent accidental phrase gluing).
                    if (target.length() > 0) {
                        target.append(" . ");
                    }
                    target.append(appendContent.toString());
                }
            }

            Document doc = new Document(
                    title.toString(),
                    content.toString(),
                    url.toString(),
                    null /* TODO: language; should be mappable/ configurable? */,
                    hit.id());

            documents.add(doc);
        }

        return documents;
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
