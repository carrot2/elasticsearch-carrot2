package org.carrot2.elasticsearch;

import java.util.List;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.assertj.core.api.Assertions;
import org.carrot2.clustering.stc.STCClusteringAlgorithm;
import org.carrot2.elasticsearch.ClusteringAction.RestClusteringAction;
import org.elasticsearch.common.xcontent.XContentType;

/** REST API tests for {@link ClusteringAction}. */
public class ClusteringActionRestIT extends SampleIndexTestCase {

  private XContentType xtype = randomFrom(XContentType.values());
  private ContentType contentType = ContentType.parse(xtype.mediaType());

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
  private void dumpClusters(List<Map<String, ?>> clusterList, int indent) {
    for (Map<String, ?> cluster : clusterList) {
      float score = ((Number) cluster.get("score")).floatValue();
      String label = (String) cluster.get("label");
      List<?> documents = (List<?>) cluster.get("documents");

      StringBuilder stringBuilder = new StringBuilder();
      for (int i = 0; i < indent; i++) {
        stringBuilder.append("    ");
      }

      List<Map<String, ?>> subclusters = (List<Map<String, ?>>) cluster.get("clusters");

      logger.debug(
          stringBuilder
              + "> "
              + label
              + " (score="
              + score
              + ", documents="
              + (documents == null ? 0 : documents.size())
              + ", subclusters="
              + (subclusters == null ? 0 : subclusters.size()));

      if (subclusters != null) {
        dumpClusters(subclusters, indent + 1);
      }
    }
  }

  private Map<?, ?> post(String queryJsonResource) throws Exception {
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");

      post.setEntity(new ByteArrayEntity(jsonResourceAs(queryJsonResource, xtype), contentType));
      HttpResponse response = httpClient.execute(post);

      Map<?, ?> map = checkHttpResponseContainsClusters(response);

      List<?> clusterList = (List<?>) map.get("clusters");
      Assertions.assertThat(clusterList).isNotNull().isNotEmpty();

      Assertions.assertThat(clusterList.size()).isGreaterThan(5);

      return map;
    }
  }

  public void testGetClusteringRequest() throws Exception {
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      HttpGet get =
          new HttpGet(
              restBaseUrl
                  + "/"
                  + RestClusteringAction.NAME
                  + "?pretty=true"
                  // search-specific attrs
                  + "&q=data+mining"
                  + "&_source=url,title,content"
                  + "&size=100"
                  // clustering-specific attrs
                  + "&query_hint=data+mining"
                  + "&"
                  + ClusteringActionRequest.JSON_CREATE_UNGROUPED_CLUSTER
                  + "=true"
                  + "&field_mapping_content=_source.title,_source.content"
                  + "&algorithm="
                  + STCClusteringAlgorithm.NAME);
      HttpResponse response = httpClient.execute(get);

      Map<?, ?> map = checkHttpResponseContainsClusters(response);

      List<?> clusterList = (List<?>) map.get("clusters");
      Assertions.assertThat(clusterList).isNotNull().isNotEmpty();

      Assertions.assertThat(clusterList.size()).isGreaterThan(5);
    }
  }

  public void testRestApiPathParams() throws Exception {
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      HttpPost post =
          new HttpPost(
              restBaseUrl
                  + "/"
                  + INDEX_EMPTY
                  + "/empty/"
                  + RestClusteringAction.NAME
                  + "?pretty=true");
      post.setEntity(
          new ByteArrayEntity(jsonResourceAs("post_with_fields.json", xtype), contentType));
      HttpResponse response = httpClient.execute(post);
      Map<?, ?> map = checkHttpResponseContainsClusters(response);

      List<?> clusterList = (List<?>) map.get("clusters");
      Assertions.assertThat(clusterList).isNotNull().isEmpty();
    }
  }

  public void testRestApiRuntimeAttributes() throws Exception {
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
      post.setEntity(
          new ByteArrayEntity(jsonResourceAs("post_runtime_attributes.json", xtype), contentType));
      HttpResponse response = httpClient.execute(post);
      Map<?, ?> map = checkHttpResponseContainsClusters(response);

      List<?> clusterList = (List<?>) map.get("clusters");
      Assertions.assertThat(clusterList).isNotNull();
      Assertions.assertThat(clusterList.size())
          .isBetween(1, /* max. cluster size cap */ 5 + /* other topics */ 1);
    }
  }

  public void testLanguageField() throws Exception {
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
      post.setEntity(
          new ByteArrayEntity(jsonResourceAs("post_language_field.json", xtype), contentType));
      HttpResponse response = httpClient.execute(post);
      Map<?, ?> map = checkHttpResponseContainsClusters(response);

      List<?> clusterList = (List<?>) map.get("clusters");
      Assertions.assertThat(clusterList.size()).isGreaterThan(1);

      Map<?, ?> info = (Map<?, ?>) map.get("info");
      Assertions.assertThat(((String) info.get("languages")).split(",")).hasSizeGreaterThan(3);
    }
  }

  public void testNonexistentFields() throws Exception {
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
      post.setEntity(
          new ByteArrayEntity(jsonResourceAs("post_nonexistent_fields.json", xtype), contentType));
      HttpResponse response = httpClient.execute(post);
      Map<?, ?> map = checkHttpResponseContainsClusters(response);

      List<?> clusterList = (List<?>) map.get("clusters");
      Assertions.assertThat(clusterList).isNotNull();
    }
  }

  public void testNonexistentAlgorithmId() throws Exception {
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
      post.setEntity(
          new ByteArrayEntity(
              jsonResourceAs("post_nonexistent_algorithmId.json", xtype), contentType));
      HttpResponse response = httpClient.execute(post);
      expectErrorResponseWithMessage(
          response, HttpStatus.SC_BAD_REQUEST, "No such algorithm: _nonexistent_");
    }
  }

  public void testInvalidSearchQuery() throws Exception {
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
      post.setEntity(
          new ByteArrayEntity(jsonResourceAs("post_invalid_query.json", xtype), contentType));
      HttpResponse response = httpClient.execute(post);
      expectErrorResponseWithMessage(response, HttpStatus.SC_BAD_REQUEST, "parsing_exception");
    }
  }

  public void testPropagatingAlgorithmException() throws Exception {
    try (CloseableHttpClient httpClient = HttpClientBuilder.create().build()) {
      HttpPost post = new HttpPost(restBaseUrl + "/" + RestClusteringAction.NAME + "?pretty=true");
      post.setEntity(
          new ByteArrayEntity(
              jsonResourceAs("post_invalid_attribute_value.json", xtype), contentType));
      HttpResponse response = httpClient.execute(post);
      expectErrorResponseWithMessage(
          response, HttpStatus.SC_INTERNAL_SERVER_ERROR, "Clustering error: Value must be <= 1.0");
    }
  }
}
