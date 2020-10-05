package org.carrot2.elasticsearch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.assertj.core.api.Assertions;
import org.carrot2.elasticsearch.ClusteringAction.ClusteringActionRequestBuilder;
import org.carrot2.elasticsearch.ClusteringAction.ClusteringActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

/** Java API tests. */
public class MultithreadedClusteringIT extends SampleIndexTestCase {

  public void testRequestFlood() throws Exception {
    final Client client = client();

    List<Callable<ClusteringActionResponse>> tasks = new ArrayList<>();

    final int requests = 100;
    final int threads = 10;

    logger.debug("Stress testing: " + client.getClass().getSimpleName() + "| ");
    for (int i = 0; i < requests; i++) {
      tasks.add(
          () -> {
            logger.debug(">");

            ClusteringActionResponse result =
                new ClusteringActionRequestBuilder(client)
                    .setQueryHint("data mining")
                    .addFieldMapping("title", LogicalField.TITLE)
                    .addHighlightedFieldMapping("content", LogicalField.CONTENT)
                    .setSearchRequest(
                        client
                            .prepareSearch()
                            .setIndices(INDEX_TEST)
                            .setTypes("test")
                            .setSize(100)
                            .setQuery(QueryBuilders.termQuery("content", "data"))
                            .highlighter(
                                new HighlightBuilder().preTags("").postTags("").field("content"))
                            .storedFields("title"))
                    .execute()
                    .actionGet();

            logger.debug("<");
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
      logger.debug("Done.");
    }
  }
}
