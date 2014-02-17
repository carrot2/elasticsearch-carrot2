package org.carrot2.elasticsearch;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.carrot2.core.LanguageCode;
import org.carrot2.elasticsearch.ClusteringAction.ClusteringActionResponse;
import org.carrot2.elasticsearch.ClusteringAction.ClusteringActionResponse.Fields;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.network.NetworkUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.TransportService;
import org.fest.assertions.api.Assertions;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.DataProvider;
import org.testng.collections.Maps;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

public class AbstractApiTest {
    protected static Node node;

    protected static Client localClient;
    protected static Client transportClient;
    
    protected static TransportAddress transportAddr;
    protected static TransportAddress restAddr;

    protected static String restBaseUrl; 

    protected final static String INDEX_NAME = "test";

    @DataProvider(name = "clients")
    public static Object[][] clientProvider() {
        return new Object[][] {
                {localClient},
                {transportClient},
        };
    }

    @DataProvider(name = "postOrGet")
    public static Object[][] postOrGet() {
        return new Object[][] {{Method.POST}, {Method.GET}};
    }
    
    @DataProvider(name = "xcontentTypes")
    public static Object[][] xcontentTypes() {
        return new Object[][] {
                {XContentType.JSON}, 
                {XContentType.SMILE}, 
                {XContentType.YAML}};
    }

    @BeforeSuite
    public static void beforeClass() throws IOException {
        node = nodeBuilder().settings(settingsBuilder()
                // Setup thread pool policy consistent across machines.
                .put("threadpool.search.type", "fixed")
                .put("threadpool.search.size", "4")
                .put("threadpool.search.queue_size", "100")
                .put("threadpool.search.reject_policy", "caller")

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
            QuerySourceBuilder querySourceBuilder = new QuerySourceBuilder();
            querySourceBuilder.setQuery(QueryBuilders.matchAllQuery());
            node.client().deleteByQuery(
                    Requests.deleteByQueryRequest(INDEX_NAME).source(querySourceBuilder)).actionGet();
        }

        // Index some sample "documents".
        Random rnd = new Random(0xdeadbeef);
        LanguageCode [] languages = LanguageCode.values();
        Collections.shuffle(Arrays.asList(languages), rnd);

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
                            .field("lang", LanguageCode.ENGLISH.getIsoCode())
                            .field("rndlang", languages[rnd.nextInt(languages.length)].getIsoCode()) 
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
    protected static void checkJsonSerialization(ClusteringActionResponse result) throws IOException {
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
     * Check for valid {@link ClusteringActionResponse}.
     */
    protected static void checkValid(ClusteringActionResponse result) {
        Assertions.assertThat(result.getDocumentGroups())
            .isNotNull()
            .isNotEmpty();

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

        Assertions.assertThat(result.getInfo())
            .containsKey(ClusteringActionResponse.Fields.Info.ALGORITHM)
            .containsKey(ClusteringActionResponse.Fields.Info.CLUSTERING_MILLIS)
            .containsKey(ClusteringActionResponse.Fields.Info.SEARCH_MILLIS)
            .containsKey(ClusteringActionResponse.Fields.Info.TOTAL_MILLIS);
    }
    

    protected static Map<String, Object> checkHttpResponseContainsClusters(HttpResponse response) throws IOException {
        Map<String, Object> map = checkHttpResponse(response);

        // We should have some clusters.
        Assertions.assertThat(map).containsKey("clusters");
        return map;
    }

    protected static Map<String, Object> checkHttpResponse(HttpResponse response) throws IOException {
        String responseString = new String(
                ByteStreams.toByteArray(response.getEntity().getContent()), 
                Charsets.UTF_8); 
    
        String responseDescription = 
                "HTTP response status: " + response.getStatusLine().toString() + ", " + 
                "HTTP body: " + responseString;
    
        Assertions.assertThat(response.getStatusLine().getStatusCode())
            .describedAs(responseDescription)
            .isEqualTo(HttpStatus.SC_OK);
    
        XContentParser parser = JsonXContent.jsonXContent.createParser(responseString);
        Map<String, Object> map = parser.mapAndClose();
        Assertions.assertThat(map)
            .describedAs(responseDescription)
            .doesNotContainKey("error");

        return map; 
    }

    protected static void expectErrorResponseWithMessage(HttpResponse response, int expectedStatus, String messageSubstring) throws IOException {
        byte[] responseBytes = ByteStreams.toByteArray(response.getEntity().getContent());
        String responseString = new String(responseBytes, Charsets.UTF_8); 
            String responseDescription = 
                "HTTP response status: " + response.getStatusLine().toString() + ", " + 
                "HTTP body: " + responseString;

        Assertions.assertThat(response.getStatusLine().getStatusCode())
            .describedAs(responseDescription)
            .isEqualTo(expectedStatus);

        XContent xcontent = XContentFactory.xContent(responseBytes);
        XContentParser parser = xcontent.createParser(responseBytes);
        Map<String, Object> responseJson = parser.mapOrderedAndClose();
        
        Assertions.assertThat(responseJson)
            .describedAs(responseString)
            .containsKey("error");

        Assertions.assertThat((String) responseJson.get("error"))
            .describedAs(responseString)
            .contains(messageSubstring);
    }

    protected static byte[] resourceAs(String resourceName, XContentType type) throws IOException {
        byte [] bytes = resource(resourceName);

        XContent xcontent = XContentFactory.xContent(bytes);
        XContentParser parser = xcontent.createParser(bytes);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XContentBuilder builder = XContentFactory.contentBuilder(type, baos).copyCurrentStructure(parser);
        builder.close();

        return bytes;
    }

    protected static byte[] resource(String resourceName) throws IOException {
        return Resources.toByteArray(Resources.getResource(ClusteringActionRestTests.class, resourceName));
    }    
}
