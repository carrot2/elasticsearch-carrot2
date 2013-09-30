package org.carrot2.elasticsearch;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.carrot2.core.LanguageCode;
import org.carrot2.elasticsearch.ClusteringAction.RestClusteringAction;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestRequest.Method;
import org.fest.assertions.api.Assertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;

/**
 * 
 */
public class RestApiTests extends AbstractApiTest {
    @DataProvider(name = "postJsonResources")
    public static Object[][] postJsonResources() {
        List<Object[]> parameters = Lists.newArrayList();
        for (String resourceName : new String [] {
                "post_with_fields.json",
                "post_with_source_fields.json",
                "post_with_highlighted_fields.json",
                "post_multiple_field_mapping.json",
                "post_cluster_by_url.json"
        }) {
            for (XContentType type : XContentType.values()) {
                parameters.add(new Object [] {resourceName, type});
            }
        }

        return parameters.toArray(new Object[parameters.size()][]);
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

    @SuppressWarnings("unchecked")
    @Test(dataProvider = "postOrGet")
    public void testListAlgorithms(Method method) throws IOException {
        final DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            HttpRequestBase request;
            String requestString = restBaseUrl + "/" 
                    + ListAlgorithmsAction.RestListAlgorithmsAction.NAME + "?pretty=true";

            switch (method) {
                case POST:
                    request = new HttpPost(requestString);
                    break;

                case GET:
                    request = new HttpGet(requestString);
                    break;

                default: throw Preconditions.unreachable();
            }
            
            HttpResponse response = httpClient.execute(request);
            Map<?,?> map = checkHttpResponse(response);

            // Check that we do have some algorithms.
            Assertions.assertThat(map.get("algorithms"))
                .describedAs("A list of algorithms")
                .isInstanceOf(List.class);

            Assertions.assertThat((List<String>) map.get("algorithms"))
                .describedAs("A list of algorithms")
                .contains("stc", "lingo", "kmeans");            
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
    }    

    @Test(dataProvider = "postJsonResources")
    public void testRestApiViaPostBody(String queryJsonResource, XContentType type) throws Exception {
        final DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
            post.setEntity(new ByteArrayEntity(resourceAs(queryJsonResource, type)));
            HttpResponse response = httpClient.execute(post);
            
            Map<?,?> map = checkHttpResponseContainsClusters(response);

            List<?> clusterList = (List<?>) map.get("clusters");
            Assertions.assertThat(clusterList)
                .isNotNull()
                .isNotEmpty();

            Assertions.assertThat(clusterList.size())
                .isGreaterThan(5);
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
    }

    @Test(dataProvider = "xcontentTypes")
    public void testRestApiPathParams(XContentType type) throws Exception {
        final DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            HttpPost post = new HttpPost(restBaseUrl 
                    + "/" + INDEX_NAME 
                    + "/empty/" 
                    + RestClusteringAction.NAME + "?pretty=true");
            post.setEntity(new ByteArrayEntity(resourceAs("post_with_fields.json", type)));
            HttpResponse response = httpClient.execute(post);
            Map<?,?> map = checkHttpResponseContainsClusters(response);

            List<?> clusterList = (List<?>) map.get("clusters");
            Assertions.assertThat(clusterList)
                .isNotNull()
                .isEmpty();
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
    }

    @Test(dataProvider = "xcontentTypes")
    public void testRestApiRuntimeAttributes(XContentType type) throws Exception {
        final DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
            post.setEntity(new ByteArrayEntity(resourceAs("post_runtime_attributes.json", type)));
            HttpResponse response = httpClient.execute(post);
            Map<?,?> map = checkHttpResponseContainsClusters(response);

            List<?> clusterList = (List<?>) map.get("clusters");
            Assertions.assertThat(clusterList)
                .isNotNull();
            Assertions.assertThat(clusterList)
                .hasSize(/* max. cluster size cap */ 5 + /* other topics */ 1);
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
    }

    @Test(dataProvider = "xcontentTypes")
    public void testLanguageField(XContentType type) throws IOException {
        final DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
            post.setEntity(new ByteArrayEntity(resourceAs("post_language_field.json", type)));
            HttpResponse response = httpClient.execute(post);
            Map<?,?> map = checkHttpResponseContainsClusters(response);

            // Check top level clusters labels.
            Set<String> allLanguages = Sets.newHashSet();
            for (LanguageCode code : LanguageCode.values()) {
                allLanguages.add(code.toString());
            }

            List<?> clusterList = (List<?>) map.get("clusters");
            for (Object o : clusterList) {
                @SuppressWarnings("unchecked")
                Map<String, Object> cluster = (Map<String, Object>) o; 
                allLanguages.remove(cluster.get("label"));
            }
            
            Assertions.assertThat(allLanguages.size())
                .describedAs("Expected a lot of languages to appear in top groups.")
                .isLessThan(LanguageCode.values().length / 2);            
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
    }
    
    @Test(dataProvider = "xcontentTypes")
    public void testNonexistentFields(XContentType type) throws Exception {
        final DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
            post.setEntity(new ByteArrayEntity(resourceAs("post_nonexistent_fields.json", type)));
            HttpResponse response = httpClient.execute(post);
            Map<?,?> map = checkHttpResponseContainsClusters(response);

            List<?> clusterList = (List<?>) map.get("clusters");
            Assertions.assertThat(clusterList).isNotNull();
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
    }

    @Test(dataProvider = "xcontentTypes")
    public void testNonexistentAlgorithmId(XContentType type) throws Exception {
        final DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
            post.setEntity(new ByteArrayEntity(resourceAs("post_nonexistent_algorithmId.json", type)));
            HttpResponse response = httpClient.execute(post);
            expectErrorResponseWithMessage(
                    response,
                    HttpStatus.SC_BAD_REQUEST,
                    "ElasticSearchIllegalArgumentException[No such algorithm: _nonexistent_]");
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
    }    

    @Test(dataProvider = "xcontentTypes")
    public void testInvalidSearchQuery(XContentType type) throws Exception {
        final DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
            post.setEntity(new ByteArrayEntity(resourceAs("post_invalid_query.json", type)));
            HttpResponse response = httpClient.execute(post);
            expectErrorResponseWithMessage(
                    response, 
                    HttpStatus.SC_BAD_REQUEST, 
                    "QueryParsingException");
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
    }    

    @Test(dataProvider = "xcontentTypes")
    public void testPropagatingAlgorithmException(XContentType type) throws Exception {
        final DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
            post.setEntity(new ByteArrayEntity(resourceAs("post_invalid_attribute_value.json", type)));
            HttpResponse response = httpClient.execute(post);
            expectErrorResponseWithMessage(
                    response, 
                    HttpStatus.SC_INTERNAL_SERVER_ERROR, 
                    "Search results clustering error:");
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
    }

    protected void expectErrorResponseWithMessage(HttpResponse response, int expectedStatus, String messageSubstring) throws IOException {
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

    protected static Map<String, Object> checkHttpResponseContainsClusters(HttpResponse response) throws IOException {
        Map<String, Object> map = checkHttpResponse(response);

        // We should have some clusters.
        Assertions.assertThat(map).containsKey("clusters");
        return map;
    }

    private static Map<String, Object> checkHttpResponse(HttpResponse response) throws IOException {
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

    private static byte[] resourceAs(String resourceName, XContentType type) throws IOException {
        byte [] bytes = resource(resourceName);

        XContent xcontent = XContentFactory.xContent(bytes);
        XContentParser parser = xcontent.createParser(bytes);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XContentBuilder builder = XContentFactory.contentBuilder(type, baos).copyCurrentStructure(parser);
        builder.close();

        return bytes;
    }

    private static byte[] resource(String resourceName) throws IOException {
        return Resources.toByteArray(Resources.getResource(RestApiTests.class, resourceName));
    }    
}
