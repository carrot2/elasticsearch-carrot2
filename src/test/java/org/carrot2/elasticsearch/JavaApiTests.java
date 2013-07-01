package org.carrot2.elasticsearch;

import java.io.IOException;

import org.carrot2.elasticsearch.ClusteringActionRequestBuilder;
import org.carrot2.elasticsearch.ClusteringActionResponse;
import org.carrot2.elasticsearch.LogicalField;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.testng.annotations.Test;

/**
 * Java API tests.
 */
public class JavaApiTests extends AbstractApiTest {
    @Test
    public void testJavaApiViaLocalClient() throws Exception {
        testApiViaClient(localClient);
    }

    @Test
    public void testJavaApiViaTransportClient() throws Exception {
        testApiViaClient(transportClient);
    }

    private static void testApiViaClient(Client client) throws IOException {
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
                    .setHighlighterPreTags("")
                    .setHighlighterPostTags("")
                    .addField("title")
                    .addHighlightedField("content"))
            .execute().actionGet();
    
        checkValid(result);
        checkJsonSerialization(result);
    }
}
