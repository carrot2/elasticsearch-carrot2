package org.carrot2.elasticsearch;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.Logger;
import org.assertj.core.api.Assertions;
import org.carrot2.core.LanguageCode;
import org.carrot2.elasticsearch.ClusteringAction.RestClusteringAction;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * REST API tests for {@link ClusteringAction}.
 */
public class ClusteringActionRestIT extends SampleIndexTestCase {

    Logger LOGGER = Loggers.getLogger(ClusteringActionRestIT.class);

    XContentType type = randomFrom(XContentType.values());

    public void testPostClusterByUrl() throws Exception {
        post("post_cluster_by_url.json");
    }
    
    public void testPostMultipleFieldMapping() throws Exception {
        post("post_multiple_field_mapping.json");
    }
    
    public void testPostWithHighlightedFields() throws Exception {
        post("post_with_highlighted_fields.json");
    }

    public void testPostWithFields() throws Exception {
        post("post_with_fields.json");
    }

    public void testPostWithSourceFields() throws Exception {
        post("post_with_source_fields.json");
    }

    @SuppressWarnings("unchecked")
    @Lingo3G
    public void testPostWithClusters() throws Exception {
        Map<?, ?> response = post("post_with_clusters.json");
        
        List<Map<String, ?>> clusterList = (List<Map<String, ?>>) response.get("clusters");
        int indent = 0;
        dumpClusters(clusterList, indent);
    }

    @SuppressWarnings("unchecked")
    void dumpClusters(List<Map<String, ?>> clusterList, int indent) {
      for (Map<String, ?> cluster : clusterList) {
        float score = ((Number) cluster.get("score")).floatValue();
        String label = (String) cluster.get("label");
        List<?> documents = (List<?>) cluster.get("documents");

        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < indent; i++) {
          stringBuilder.append("    ");
        }
        
        List<Map<String, ?>> subclusters = (List<Map<String, ?>>) cluster.get("clusters");

        LOGGER.debug(stringBuilder + "> " + label + " (score=" + score
            + ", documents=" + (documents == null ? 0 : documents.size()) 
            + ", subclusters=" + (subclusters == null ? 0 : subclusters.size()));

        if (subclusters != null) {
          dumpClusters(subclusters, indent + 1);
        }
      }
    }

    protected Map<?,?> post(String queryJsonResource) throws Exception {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
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

            return map;
        }
    }

    public void testGetClusteringRequest() throws Exception {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            HttpGet get = new HttpGet(restBaseUrl + "/" + RestClusteringAction.NAME 
                    + "?pretty=true" 
                    // search-specific attrs
                    + "&q=data+mining"
                    + "&_source=url,title,content"
                    + "&size=100"
                    // clustering-specific attrs
                    + "&query_hint=data+mining"
                    + "&field_mapping_url=_source.url"
                    + "&field_mapping_content=_source.title,_source.content"
                    + "&algorithm=stc");
            HttpResponse response = httpClient.execute(get);

            Map<?,?> map = checkHttpResponseContainsClusters(response);

            List<?> clusterList = (List<?>) map.get("clusters");
            Assertions.assertThat(clusterList)
                .isNotNull()
                .isNotEmpty();

            Assertions.assertThat(clusterList.size())
                .isGreaterThan(5);
        }
    }
    
    public void testRestApiPathParams() throws Exception {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
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
        }
    }    
    
    public void testRestApiRuntimeAttributes() throws Exception {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
            post.setEntity(new ByteArrayEntity(resourceAs("post_runtime_attributes.json", type)));
            HttpResponse response = httpClient.execute(post);
            Map<?,?> map = checkHttpResponseContainsClusters(response);

            List<?> clusterList = (List<?>) map.get("clusters");
            Assertions.assertThat(clusterList)
                .isNotNull();
            Assertions.assertThat(clusterList)
                .hasSize(/* max. cluster size cap */ 5 + /* other topics */ 1);
        }
    }
    
    public void testLanguageField() throws Exception {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
            post.setEntity(new ByteArrayEntity(resourceAs("post_language_field.json", type)));
            HttpResponse response = httpClient.execute(post);
            Map<?,?> map = checkHttpResponseContainsClusters(response);

            // Check top level clusters labels.
            Set<String> allLanguages = new HashSet<>();
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
        }
    }
    
    public void testNonexistentFields() throws Exception {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
            post.setEntity(new ByteArrayEntity(resourceAs("post_nonexistent_fields.json", type)));
            HttpResponse response = httpClient.execute(post);
            Map<?,?> map = checkHttpResponseContainsClusters(response);

            List<?> clusterList = (List<?>) map.get("clusters");
            Assertions.assertThat(clusterList).isNotNull();
        }
    }

    public void testNonexistentAlgorithmId() throws Exception {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
            post.setEntity(new ByteArrayEntity(resourceAs("post_nonexistent_algorithmId.json", type)));
            HttpResponse response = httpClient.execute(post);
            expectErrorResponseWithMessage(
                    response,
                    HttpStatus.SC_BAD_REQUEST,
                    "No such algorithm: _nonexistent_");
        }
    }    

    public void testInvalidSearchQuery() throws Exception {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
            post.setEntity(new ByteArrayEntity(resourceAs("post_invalid_query.json", type)));
            HttpResponse response = httpClient.execute(post);
            expectErrorResponseWithMessage(
                    response, 
                    HttpStatus.SC_BAD_REQUEST, 
                    "parsing_exception");
        }
    }    

    public void testPropagatingAlgorithmException() throws Exception {
        try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
            HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
            post.setEntity(new ByteArrayEntity(resourceAs("post_invalid_attribute_value.json", type)));
            HttpResponse response = httpClient.execute(post);
            expectErrorResponseWithMessage(
                    response, 
                    HttpStatus.SC_INTERNAL_SERVER_ERROR, 
                    "Search results clustering error:");
        }
    }    
}
