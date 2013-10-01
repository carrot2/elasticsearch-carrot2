package org.carrot2.elasticsearch;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;
import org.elasticsearch.rest.RestRequest.Method;
import org.fest.assertions.api.Assertions;
import org.testng.annotations.Test;

/**
 * REST API tests for {@link ListAlgorithmsAction}.
 */
public class ListAlgorithmsActionRestTests extends AbstractApiTest {
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
}
