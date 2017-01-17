package org.carrot2.elasticsearch;

import org.apache.logging.log4j.Logger;
import org.assertj.core.api.Assertions;
import org.carrot2.elasticsearch.ClusteringAction.ClusteringActionRequestBuilder;
import org.carrot2.elasticsearch.ClusteringAction.ClusteringActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Java API tests.
 */
public class MultithreadedClusteringIT extends SampleIndexTestCase {

    private static final Logger LOGGER = Loggers.getLogger(MultithreadedClusteringIT.class);

    public void testRequestFlood() throws Exception {
        final Client client = client();

        List<Callable<ClusteringActionResponse>> tasks = new ArrayList<>();

        final int requests = 100;
        final int threads = 10;

        LOGGER.debug("Stress testing: " + client.getClass().getSimpleName() + "| ");
        for (int i = 0; i < requests; i++) {
            tasks.add(() -> {
                LOGGER.debug(">");

                ClusteringActionResponse result = new ClusteringActionRequestBuilder(client)
                    .setQueryHint("data mining")
                    .addFieldMapping("title", LogicalField.TITLE)
                    .addHighlightedFieldMapping("content", LogicalField.CONTENT)
                    .setSearchRequest(
                      client.prepareSearch()
                            .setIndices(INDEX_NAME)
                            .setTypes("test")
                            .setSize(100)
                            .setQuery(QueryBuilders.termQuery("_all", "data"))
                            .highlighter(new HighlightBuilder().preTags("").postTags("").field("content"))
                            .storedFields("title"))
                    .execute().actionGet();

                LOGGER.debug("<");
                checkValid(result);
                checkJsonSerialization(result);
                return result;
            });
        }

        ExecutorService executor = Executors.newFixedThreadPool(threads);
        try {
            for (Future<ClusteringActionResponse> future : executor.invokeAll(tasks)) {
                ClusteringActionResponse response = future.get();
                Assertions.assertThat(response).isNotNull();
                Assertions.assertThat(response.getSearchResponse()).isNotNull();
            }
        } finally {
            executor.shutdown();
            LOGGER.debug("Done.");
        }
    }
}
