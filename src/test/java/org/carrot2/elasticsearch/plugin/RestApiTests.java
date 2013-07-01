package org.carrot2.elasticsearch.plugin;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.fest.assertions.api.Assertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;

/**
 * 
 */
public class RestApiTests extends AbstractApiTest {
    @DataProvider(name = "postJsonResources")
    public static Object[][] postJsonResources() {
        return new Object[][] {
                {"post_with_fields.json"},
                {"post_with_source_fields.json"},
                {"post_with_highlighted_fields.json"},
                {"post_multiple_field_mapping.json"},
                {"post_cluster_by_url.json"},
        };
    }

    @Test(dataProvider = "postJsonResources")
    public void testRestApiViaPostBody(String queryJsonResource) throws Exception {
        final DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            String requestJson = resourceToString(queryJsonResource);

            HttpPost post = new HttpPost(restBaseUrl + "/" + RestCarrot2ClusteringAction.NAME + "?pretty=true");
            post.setEntity(new StringEntity(requestJson, Charsets.UTF_8));
            HttpResponse response = httpClient.execute(post);
            
            Map<?,?> map = checkHttpResponse(response);

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

    @Test
    public void testRestApiPathParams() throws Exception {
        final DefaultHttpClient httpClient = new DefaultHttpClient();
        try {
            String requestJson = resourceToString("post_with_fields.json");

            HttpPost post = new HttpPost(restBaseUrl 
                    + "/" + INDEX_NAME 
                    + "/empty/" 
                    + RestCarrot2ClusteringAction.NAME + "?pretty=true");
            post.setEntity(new StringEntity(requestJson, Charsets.UTF_8));
            HttpResponse response = httpClient.execute(post);
            Map<?,?> map = checkHttpResponse(response);

            List<?> clusterList = (List<?>) map.get("clusters");
            Assertions.assertThat(clusterList)
                .isNotNull()
                .isEmpty();
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
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
    
        // We should have some clusters.
        Assertions.assertThat(map)
            .describedAs(responseDescription)
            .containsKey("clusters");
    
        return map;
    }

    protected String resourceToString(String resourceName) throws IOException {
        return Resources.toString(
                Resources.getResource(this.getClass(), resourceName),
                Charsets.UTF_8);
    }    
}
