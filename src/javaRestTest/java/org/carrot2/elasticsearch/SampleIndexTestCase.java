package org.carrot2.elasticsearch;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.assertj.core.api.Assertions;
import org.carrot2.elasticsearch.ClusteringActionResponse.Fields;
import org.carrot2.language.LanguageComponentsLoader;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

/** Perform tests on sample data. */
public abstract class SampleIndexTestCase extends ESIntegTestCase {
  protected String restBaseUrl;
  protected Client client;

  @Override
  protected Collection<Class<? extends Plugin>> nodePlugins() {
    return Collections.singletonList(ClusteringPlugin.class);
  }

  protected static final String INDEX_TEST = "test";
  protected static final String INDEX_EMPTY = "empty";

  @Before
  public void createTestIndex() throws Exception {
    // Delete any previously indexed content.
    client = client();
    if (!client.admin().indices().prepareCreate(INDEX_TEST).get().isAcknowledged()) {
      String testTemplate =
          "{"
              + "  \"test\": {"
              + "    \"properties\": {"
              + "      \"url\": { \"type\": \"text\" },"
              + "      \"title\": { \"type\": \"text\" },"
              + "      \"content\": { \"type\": \"text\" },"
              + "      \"lang\": { \"type\": \"text\" },"
              + "      \"rndlang\": { \"type\": \"text\" }"
              + "    }"
              + "  }"
              + "}";

      String emptyTemplate =
          "{"
              + "  \"empty\": {"
              + "    \"properties\": {"
              + "      \"url\": { \"type\": \"text\" },"
              + "      \"title\": { \"type\": \"text\" },"
              + "      \"content\": { \"type\": \"text\" },"
              + "      \"lang\": { \"type\": \"text\" },"
              + "      \"rndlang\": { \"type\": \"text\" }"
              + "    }"
              + "  }"
              + "}";

      CreateIndexResponse response =
          client.admin().indices().prepareCreate(INDEX_TEST).setMapping("test", testTemplate).get();
      Assertions.assertThat(response.isAcknowledged()).isTrue();

      response =
          client
              .admin()
              .indices()
              .prepareCreate(INDEX_EMPTY)
              .setMapping("empty", emptyTemplate)
              .get();
      Assertions.assertThat(response.isAcknowledged()).isTrue();

      // Create content at random in the test index.
      Random rnd = random();
      String[] languages = new LanguageComponentsLoader().load().languages().toArray(String[]::new);
      Arrays.sort(languages);

      BulkRequestBuilder bulk = client.prepareBulk();
      TestInfra.load("datamining.json").stream()
          .map(
              doc ->
                  doc.cloneWith(
                      Map.ofEntries(
                          Map.entry("lang", "English"),
                          Map.entry("rndlang", languages[rnd.nextInt(languages.length)]))))
          .forEach(
              doc -> {
                bulk.add(client.prepareIndex().setIndex(INDEX_TEST).setSource(doc.toXContent()));
              });

      bulk.add(
          client
              .prepareIndex()
              .setIndex(INDEX_EMPTY)
              .setSource(
                  new TestInfra.TestDocument(Map.of("url", "", "title", "", "content", ""))
                      .toXContent()));

      bulk.execute().actionGet();
      flushAndRefresh(INDEX_TEST);
      flushAndRefresh(INDEX_EMPTY);
    }
    ensureGreen(INDEX_TEST);
    ensureGreen(INDEX_EMPTY);

    InetSocketAddress endpoint = randomFrom(cluster().httpAddresses());
    this.restBaseUrl = "http://" + NetworkAddress.format(endpoint);
  }

  /** Check for valid {@link ClusteringActionResponse}. */
  protected static void checkValid(ClusteringActionResponse result) {
    Assertions.assertThat(result.getDocumentGroups())
        .as("top-level clusters")
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

      Assertions.assertThat(g.getLabel()).as("label").isNotNull().isNotEmpty();

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
        .containsKey(ClusteringActionResponse.Fields.Info.MAX_HITS)
        .containsKey(ClusteringActionResponse.Fields.Info.LANGUAGES);
  }

  /** Roundtrip to/from JSON. */
  protected static void checkJsonSerialization(ClusteringActionResponse result) throws IOException {
    XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
    builder.startObject();
    result.toXContent(builder, ToXContent.EMPTY_PARAMS);
    builder.endObject();
    String json = Strings.toString(builder);

    XContentParserConfiguration parserConfig =
        XContentParserConfiguration.EMPTY.withDeprecationHandler(
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION);
    try (XContentParser parser = JsonXContent.jsonXContent.createParser(parserConfig, json)) {
      Map<String, Object> mapAndClose = parser.map();
      Assertions.assertThat(mapAndClose).as("json-result").containsKey(Fields.CLUSTERS);
    }
  }
}
