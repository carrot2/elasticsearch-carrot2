package org.carrot2.elasticsearch.plugin;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * 
 */
public class SimpleClusteringTest {
    private static Node node;

    @BeforeClass
    public static void setup() throws IOException {
        node = nodeBuilder().settings(settingsBuilder()
                .put("path.data", "target/data")
                .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress()))
                .local(true)
                .node();

        // Wait for the node/ cluster to come alive.
        node.client().admin().cluster().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForYellowStatus().execute().actionGet();

        // Delete any previous documents.
        final String indexName = "test";
        node.client().deleteByQuery(
                Requests.deleteByQueryRequest(indexName).query(
                        QueryBuilders.matchAllQuery())).actionGet();

        // Index some sample "documents".
        BulkRequestBuilder bulk = node.client().prepareBulk();
        for (String[] data : SampleDocumentData.SAMPLE_DATA) {
            bulk.add(new IndexRequestBuilder(node.client())
            .setIndex(indexName)
            .setType("test")
            .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                        .field("url",     data[0])
                        .field("title",   data[1])
                        .field("content", data[2])
                    .endObject()));
        }
        bulk.setRefresh(true).execute().actionGet();

        SearchResponse search = 
                node.client()
                    .prepareSearch("test")
                    .setTypes("test")
                    .setQuery(QueryBuilders.termQuery("_all", "data"))
                    .addHighlightedField("content")
                    .addHighlightedField("title")
                    .execute().actionGet();

        System.out.println(search);
    }

    @AfterClass
    public void tearDown() {
        node.close();
    }

    @Test
    public void testSimpleClustering() throws Exception {
    }
}
