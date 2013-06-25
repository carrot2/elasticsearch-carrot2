package org.carrot2.elasticsearch.plugin;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.transport.TransportService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * 
 */
public class SimpleClusteringTest {
    private static Node node;

    private static Client localClient;
    private static Client transportClient;

    private final static String INDEX_NAME = "test";

    @BeforeClass
    public static void setup() throws IOException {
        node = nodeBuilder().settings(settingsBuilder()
                .put("path.data", "target/data")
                .put("cluster.name", "test-cluster-" + NetworkUtils.getLocalAddress()))
                .node();

        // Wait for the node/ cluster to come alive.
        node.client().admin().cluster().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForYellowStatus().execute().actionGet();

        // Local client.
        localClient = node.client();

        // Use remote client.
        TransportAddress addr = ((InternalNode) node).injector()
                .getInstance(TransportService.class).boundAddress().publishAddress(); 
        transportClient = new TransportClient(ImmutableSettings.builder()
                        .put("cluster.name", node.settings().get("cluster.name"))
                        .put("client.transport.sniff", true))
                    .addTransportAddress(addr);

        // Delete any previous documents.
        if (new IndicesExistsRequestBuilder(node.client().admin().indices(), INDEX_NAME).execute().actionGet().isExists()) {
            node.client().deleteByQuery(
                    Requests.deleteByQueryRequest(INDEX_NAME).query(
                            QueryBuilders.matchAllQuery())).actionGet();
        }

        // Index some sample "documents".
        BulkRequestBuilder bulk = node.client().prepareBulk();
        for (String[] data : SampleDocumentData.SAMPLE_DATA) {
            bulk.add(new IndexRequestBuilder(node.client())
                .setIndex(INDEX_NAME)
                .setType("test")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                            .field("url",     data[0])
                            .field("title",   data[1])
                            .field("content", data[2])
                        .endObject()));
        }
        bulk.setRefresh(true).execute().actionGet();
    }

    @AfterClass
    public void tearDown() {
        transportClient.close();
        node.close();
    }

    @Test
    public void testClusteringViaApi() throws Exception {
        // TODO: parameterize this to use local/transport client.
        // TODO: add REST api tests.

        Carrot2ClusteringActionResponse result = new Carrot2ClusteringRequestBuilder(transportClient)
            .setSearchRequest(
                    node.client()
                    .prepareSearch("test")
                    .setTypes("test")
                    .setQuery(QueryBuilders.termQuery("_all", "data"))
                    .addHighlightedField("content")
                    .addHighlightedField("title"))
            .execute().actionGet(); 

        System.out.println(result);
    }
}
