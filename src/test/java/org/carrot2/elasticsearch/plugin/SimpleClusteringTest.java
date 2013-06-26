package org.carrot2.elasticsearch.plugin;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.transport.TransportService;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;

/**
 * 
 */
public class SimpleClusteringTest {
    private static Node node;

    private static Client localClient;
    private static Client transportClient;
    
    private static TransportAddress transportAddr;
    private static TransportAddress restAddr;

    private static String restBaseUrl; 

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

        localClient = node.client();

        transportAddr = ((InternalNode) node).injector()
                .getInstance(TransportService.class)
                .boundAddress()
                .publishAddress();

        transportClient = new TransportClient(ImmutableSettings.builder()
                    .put("cluster.name", node.settings().get("cluster.name"))
                    .put("client.transport.sniff", true))
                .addTransportAddress(transportAddr);

        restAddr = ((InternalNode) node).injector()
                .getInstance(HttpServerTransport.class)
                .boundAddress()
                .publishAddress();

        InetSocketAddress address = ((InetSocketTransportAddress) restAddr).address();
        restBaseUrl = "http://" + address.getHostName() + ":" + address.getPort();

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

    // @Test
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

    @Test
    public void testClusteringViaRest() throws Exception {
        final DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            // HttpPost post = new HttpPost(restBaseUrl + "/" + RestCarrot2ClusteringAction.NAME);
            HttpPost post = new HttpPost(restBaseUrl + "/_search_with_clusters?q=content:data&pretty=true&fields=title,url,content");
            post.setEntity(new StringEntity("", Charsets.UTF_8));
            HttpResponse response = httpClient.execute(post);
            System.out.println(">> " + response);
            System.out.println(">> " + new String(ByteStreams.toByteArray(response.getEntity().getContent()), "UTF-8"));
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
    }    
}
