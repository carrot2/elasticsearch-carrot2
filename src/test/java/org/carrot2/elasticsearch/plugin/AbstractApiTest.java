package org.carrot2.elasticsearch.plugin;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Map;

import org.carrot2.elasticsearch.plugin.Carrot2ClusteringActionResponse.Fields;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.TransportService;
import org.fest.assertions.api.Assertions;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.collections.Maps;

public class AbstractApiTest {
    protected static Node node;

    protected static Client localClient;
    protected static Client transportClient;
    
    protected static TransportAddress transportAddr;
    protected static TransportAddress restAddr;

    protected static String restBaseUrl; 

    protected final static String INDEX_NAME = "test";

    @BeforeSuite
    public static void beforeClass() throws IOException {
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

        bulk.add(new IndexRequestBuilder(node.client())
            .setIndex(INDEX_NAME)
            .setType("empty")
            .setSource(XContentFactory.jsonBuilder()
                    .startObject()
                        .field("url",     "")
                        .field("title",   "")
                        .field("content", "")
                    .endObject()));

        bulk.setRefresh(true).execute().actionGet();
    }

    @AfterSuite
    public static final void afterSuite() {
        transportClient.close();
        node.close();
    }
    

    /**
     * Roundtrip to/from JSON.
     */
    protected static void checkJsonSerialization(Carrot2ClusteringActionResponse result) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        result.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = builder.string();
        
        Map<String, Object> mapAndClose = JsonXContent.jsonXContent.createParser(json).mapAndClose();
        Assertions.assertThat(mapAndClose)
            .as("json-result")
            .containsKey(Fields.CLUSTERS.underscore().getValue());
    }

    /**
     * Check valid response?
     */
    protected static void checkValid(Carrot2ClusteringActionResponse result) {
        Assertions.assertThat(result.getDocumentGroups())
            .isNotNull()
            .isNotEmpty();

        // TODO: add info() checks.

        Map<String,SearchHit> idToHit = Maps.newHashMap();
        for (SearchHit hit : result.getSearchResponse().getHits()) {
            idToHit.put(hit.getId(), hit);
        }
    
        ArrayDeque<DocumentGroup> queue = new ArrayDeque<DocumentGroup>();
        queue.addAll(Arrays.asList(result.getDocumentGroups()));
        while (!queue.isEmpty()) {
            DocumentGroup g = queue.pop();
            
            Assertions.assertThat(g.getLabel())
                .as("label")
                .isNotNull()
                .isNotEmpty();
    
            String[] documentReferences = g.getDocumentReferences();
            Assertions.assertThat(idToHit.keySet())
                .as("docRefs")
                .containsAll(Arrays.asList(documentReferences));
        }
    }
}
