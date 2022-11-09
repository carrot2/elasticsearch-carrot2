package org.carrot2.elasticsearch;

import static org.carrot2.elasticsearch.TestInfra.TestDocument;
import static org.carrot2.elasticsearch.TestInfra.jsonResource;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.carrot2.clustering.stc.STCClusteringAlgorithm;
import org.carrot2.elasticsearch.ClusteringAction.RestClusteringAction;
import org.carrot2.language.LanguageComponentsLoader;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

/** REST API tests for {@link ClusteringAction}. */
public class ClusteringActionRestIT extends ESRestTestCase {
  protected static final String INDEX_RNDLANG = "test";
  protected static final String INDEX_NOLANG = "empty";

  @Before
  public void setupData() throws IOException {
    addTestData();
  }

  public void testPostMultipleFieldMapping() throws Exception {
    postNoError("post_multiple_field_mapping.json");
  }

  public void testPostWithHighlightedFields() throws Exception {
    postNoError("post_with_highlighted_fields.json");
  }

  public void testPostWithFields() throws Exception {
    postNoError("post_with_fields.json");
  }

  public void testPostWithSourceFields() throws Exception {
    postNoError("post_with_source_fields.json");
  }

  public void testGetClusteringRequest() throws Exception {
    var request = new Request("GET", "/" + INDEX_RNDLANG + "/" + RestClusteringAction.NAME);
    request.addParameter("pretty", "true");
    request.addParameter("q", "data mining");
    request.addParameter("_source", "url,title,content");
    request.addParameter("size", "100");
    request.addParameter("query_hint", "data mining");
    request.addParameter(ClusteringActionRequest.JSON_CREATE_UNGROUPED_CLUSTER, "true");
    request.addParameter("field_mapping_content", "_source.title,_source.content");
    request.addParameter("algorithm", STCClusteringAlgorithm.NAME);

    var response = checkHttpResponseContainsClusters(request);
    Assertions.assertThat((List<?>) response.get("clusters")).hasSizeGreaterThan(5);
  }

  public void testRestApiRuntimeAttributes() throws Exception {
    var response = postNoError("post_runtime_attributes.json");
    Assertions.assertThat((List<?>) response.get("clusters"))
        .hasSizeBetween(1, /* max. cluster size cap */ 5 + /* other topics */ 1);
  }

  public void testLanguageField() throws Exception {
    var response = postNoError("post_language_field.json");
    Assertions.assertThat((List<?>) response.get("clusters")).hasSizeGreaterThan(1);

    Assertions.assertThat(((String) ObjectPath.eval("info.languages", response)).split(","))
        .hasSizeGreaterThan(3);
  }

  public void testNonexistentFields() throws Exception {
    var request = new Request("POST", "/" + INDEX_RNDLANG + "/" + RestClusteringAction.NAME);
    request.addParameter("pretty", "true");

    postNoError("post_nonexistent_fields.json");
  }

  public void testNonexistentAlgorithmId() throws Exception {
    var request = new Request("POST", "/" + INDEX_RNDLANG + "/" + RestClusteringAction.NAME);
    request.addParameter("pretty", "true");
    request.setJsonEntity(jsonResource(getClass(), "post_nonexistent_algorithmId.json"));

    expectErrorResponseWithMessage(
        request, HttpURLConnection.HTTP_BAD_REQUEST, "No such algorithm: _nonexistent_");
  }

  public void testInvalidSearchQuery() throws Exception {
    var request = new Request("POST", "/" + INDEX_RNDLANG + "/" + RestClusteringAction.NAME);
    request.addParameter("pretty", "true");
    request.setJsonEntity(jsonResource(getClass(), "post_invalid_query.json"));

    expectErrorResponseWithMessage(
        request, HttpURLConnection.HTTP_BAD_REQUEST, "parsing_exception");
  }

  public void testPropagatingAlgorithmException() throws Exception {
    var request = new Request("POST", "/" + INDEX_RNDLANG + "/" + RestClusteringAction.NAME);
    request.addParameter("pretty", "true");
    request.setJsonEntity(jsonResource(getClass(), "post_invalid_attribute_value.json"));

    expectErrorResponseWithMessage(
        request, HttpURLConnection.HTTP_INTERNAL_ERROR, "Clustering error: Value must be <= 1.0");
  }

  void expectErrorResponseWithMessage(Request request, int expectedStatus, String messageSubstring)
      throws IOException {
    Response response;
    try {
      response = client().performRequest(request);
      fail("Expected response exception but received: " + response);
    } catch (ResponseException e) {
      response = e.getResponse();
    }

    byte[] responseBytes = response.getEntity().getContent().readAllBytes();
    String responseString = new String(responseBytes, StandardCharsets.UTF_8);
    String responseDescription =
        "HTTP response status: "
            + response.getStatusLine().toString()
            + ", "
            + "HTTP body: "
            + responseString;

    Assertions.assertThat(response.getStatusLine().getStatusCode())
        .describedAs(responseDescription)
        .isEqualTo(expectedStatus);

    XContentType xContentType = XContentType.fromMediaType(response.getHeader("Content-Type"));
    try (XContentParser parser =
        XContentHelper.createParser(
            NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new BytesArray(responseBytes),
            xContentType)) {
      Map<String, Object> responseJson = parser.mapOrdered();

      Assertions.assertThat(responseJson).describedAs(responseString).containsKey("error");

      Assertions.assertThat(responseJson.get("error").toString())
          .describedAs(responseString)
          .contains(messageSubstring);
    }
  }

  public Map<String, Object> postNoError(String jsonResource) throws Exception {
    var request = new Request("POST", "/" + INDEX_RNDLANG + "/" + RestClusteringAction.NAME);
    request.addParameter("pretty", "true");
    request.setJsonEntity(jsonResource(getClass(), jsonResource));
    return checkHttpResponseContainsClusters(request);
  }

  private Map<String, Object> checkHttpResponseContainsClusters(Request request)
      throws IOException {
    Map<String, Object> response = responseAsMap(client().performRequest(request));
    Object clusters = response.get("clusters");
    Assertions.assertThat(clusters).isNotNull();

    System.out.println(
        "Clusters:\n"
            + Strings.toString(
                XContentFactory.jsonBuilder()
                    .prettyPrint()
                    .startObject()
                    .field("clusters", clusters)
                    .endObject()));
    return response;
  }

  private static void addTestData() throws UnsupportedCharsetException, IOException {
    Random rnd = random();
    String[] languages = new LanguageComponentsLoader().load().languages().toArray(String[]::new);
    Arrays.sort(languages);

    List<TestDocument> docs = TestInfra.load("datamining.json");

    index(
        INDEX_RNDLANG,
        docs.stream()
            .map(
                doc ->
                    doc.cloneWith(
                        Map.ofEntries(
                            Map.entry("lang", "English"),
                            Map.entry("rndlang", languages[rnd.nextInt(languages.length)])))));

    index(INDEX_NOLANG, docs.stream());
  }

  public static void index(String index, Stream<TestDocument> docStream) throws IOException {
    if (indexExists(index)) {
      return;
    }

    List<TestDocument> docs = docStream.collect(Collectors.toList());

    Set<String> fields =
        docs.stream()
            .flatMap(doc -> doc.fields().stream().map(Map.Entry::getKey))
            .collect(Collectors.toSet());

    var xc = XContentFactory.jsonBuilder().startObject();
    for (String field : fields) {
      xc.startObject(field).field("type", "text").endObject();
    }
    xc.endObject();
    createIndex(index, Settings.EMPTY, "\"properties\": " + Strings.toString(xc));

    Request request = new Request("PUT", "/" + index + "/_bulk");
    request.addParameter("refresh", "true");

    StringBuilder bulk = new StringBuilder();
    int idx = 0;
    for (var doc : docs) {
      bulk.append(
          Strings.toString(
              XContentFactory.jsonBuilder()
                  .startObject()
                  .startObject("index")
                  .field("_id", Integer.toString(idx++))
                  .endObject()
                  .endObject()));
      bulk.append("\n");

      bulk.append(doc.toJson().replaceAll("[\r\n]+", " "));
      bulk.append("\n");
    }
    System.out.println(bulk.toString());
    request.setJsonEntity(bulk.toString());
    client().performRequest(request);
  }
}
