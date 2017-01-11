package org.carrot2.elasticsearch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.assertj.core.api.Assertions;
import org.carrot2.core.LanguageCode;
import org.carrot2.elasticsearch.ClusteringAction.ClusteringActionResponse;
import org.carrot2.elasticsearch.ClusteringAction.ClusteringActionResponse.Fields;
import org.carrot2.shaded.guava.common.base.Charsets;
import org.carrot2.shaded.guava.common.io.ByteStreams;
import org.carrot2.shaded.guava.common.io.Resources;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

/**
 * Perform tests on sample data. 
 */
public abstract class SampleIndexTestCase extends ESIntegTestCase {
    protected String restBaseUrl;
    protected Client client;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED, true)
                .build();
    }
    
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
      return Collections.singletonList(ClusteringPlugin.class);
    }
    
    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
      return nodePlugins();
    }

    protected static final String INDEX_NAME = "test";

    @Before
    public void createTestIndex() throws Exception {
        // Delete any previously indexed content.
        if (!client().admin().indices().prepareExists(INDEX_NAME).get().isExists()) {
            // Create content at random in the test index.
            Random rnd = random();
            LanguageCode [] languages = LanguageCode.values();
            Collections.shuffle(Arrays.asList(languages), rnd);

            this.client = client();
            BulkRequestBuilder bulk = client.prepareBulk();
            for (String[] data : SampleDocumentData.SAMPLE_DATA) {
                bulk.add(client.prepareIndex()
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

            bulk.add(client.prepareIndex()
                .setIndex(INDEX_NAME)
                .setType("empty")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                            .field("url",     "")
                            .field("title",   "")
                            .field("content", "")
                        .endObject()));

            bulk.execute().actionGet();
            flushAndRefresh(INDEX_NAME);
        }
        ensureGreen(INDEX_NAME);

        InetSocketAddress endpoint = randomFrom(cluster().httpAddresses());
        this.restBaseUrl = "http://" + NetworkAddress.format(endpoint);
    }

    /**
     * Check for valid {@link ClusteringActionResponse}.
     */
    protected static void checkValid(ClusteringActionResponse result) {
        Assertions.assertThat(result.getDocumentGroups())
            .isNotNull()
            .isNotEmpty();

        Map<String, SearchHit> idToHit = new HashMap<>();
        SearchHits hits = result.getSearchResponse().getHits();
        if (hits != null) {
            for (SearchHit hit : hits) {
                idToHit.put(hit.getId(), hit);
            }
        }
    
        String maxHits = result.getInfo().get(ClusteringActionResponse.Fields.Info.MAX_HITS);
        final boolean containsAllHits = 
                (maxHits == null || maxHits.isEmpty() || Integer.parseInt(maxHits) == Integer.MAX_VALUE);

        ArrayDeque<DocumentGroup> queue = new ArrayDeque<>();
        queue.addAll(Arrays.asList(result.getDocumentGroups()));
        while (!queue.isEmpty()) {
            DocumentGroup g = queue.pop();
            
            Assertions.assertThat(g.getLabel())
                .as("label")
                .isNotNull()
                .isNotEmpty();
    
            if (containsAllHits) {
                String[] documentReferences = g.getDocumentReferences();
                Assertions.assertThat(idToHit.keySet())
                    .as("docRefs")
                    .containsAll(Arrays.asList(documentReferences));
            }
        }

        Assertions.assertThat(result.getInfo())
            .containsKey(ClusteringActionResponse.Fields.Info.ALGORITHM)
            .containsKey(ClusteringActionResponse.Fields.Info.CLUSTERING_MILLIS)
            .containsKey(ClusteringActionResponse.Fields.Info.SEARCH_MILLIS)
            .containsKey(ClusteringActionResponse.Fields.Info.TOTAL_MILLIS)
            .containsKey(ClusteringActionResponse.Fields.Info.MAX_HITS);
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

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(json)) {
            Map<String, Object> mapAndClose = parser.map();
            Assertions.assertThat(mapAndClose)
                .as("json-result")
                .containsKey(Fields.CLUSTERS);
        }
    }

    protected byte[] resourceAs(String resourceName, XContentType type) throws IOException {
        byte [] bytes = resource(resourceName);

        XContent xcontent = XContentFactory.xContent(bytes);
        XContentParser parser = xcontent.createParser(bytes);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XContentBuilder builder = XContentFactory.contentBuilder(type, baos).copyCurrentStructure(parser);
        builder.close();

        return bytes;
    }

    protected byte[] resource(String resourceName) throws IOException {
        return Resources.toByteArray(
                Resources.getResource(
                        getClass(), 
                        "_" + getClass().getSimpleName() + "/" + resourceName));
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
    
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(responseString)) {
            Map<String, Object> map = parser.map();
            Assertions.assertThat(map)
                .describedAs(responseDescription)
                .doesNotContainKey("error");
            return map; 
        }
    }

    protected static void expectErrorResponseWithMessage(HttpResponse response,
                                                         int expectedStatus,
                                                         String messageSubstring) throws IOException {
        byte[] responseBytes = ByteStreams.toByteArray(response.getEntity().getContent());
        String responseString = new String(responseBytes, Charsets.UTF_8); 
            String responseDescription = 
                "HTTP response status: " + response.getStatusLine().toString() + ", " + 
                "HTTP body: " + responseString;

        Assertions.assertThat(response.getStatusLine().getStatusCode())
            .describedAs(responseDescription)
            .isEqualTo(expectedStatus);

        XContent xcontent = XContentFactory.xContent(responseBytes);
        try (XContentParser parser = xcontent.createParser(responseBytes)) {
            Map<String, Object> responseJson = parser.mapOrdered();
            
            Assertions.assertThat(responseJson)
                .describedAs(responseString)
                .containsKey("error");

            Assertions.assertThat(responseJson.get("error").toString())
                .describedAs(responseString)
                .contains(messageSubstring);
        }
    }    
}
