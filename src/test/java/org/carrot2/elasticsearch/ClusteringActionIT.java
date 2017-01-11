package org.carrot2.elasticsearch;

import org.apache.logging.log4j.Logger;
import org.assertj.core.api.Assertions;
import org.carrot2.clustering.lingo.LingoClusteringAlgorithmDescriptor;
import org.carrot2.clustering.stc.STCClusteringAlgorithmDescriptor;
import org.carrot2.core.LanguageCode;
import org.carrot2.elasticsearch.ClusteringAction.ClusteringActionRequestBuilder;
import org.carrot2.elasticsearch.ClusteringAction.ClusteringActionResponse;
import org.carrot2.elasticsearch.ListAlgorithmsAction.ListAlgorithmsActionRequestBuilder;
import org.carrot2.elasticsearch.ListAlgorithmsAction.ListAlgorithmsActionResponse;
import org.carrot2.text.clustering.MultilingualClustering.LanguageAggregationStrategy;
import org.carrot2.text.clustering.MultilingualClusteringDescriptor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * API tests for {@link ClusteringAction}.
 */
public class ClusteringActionIT extends SampleIndexTestCase {

    private static final Logger LOGGER = Loggers.getLogger(ClusteringActionIT.class);

    public void testComplexQuery() throws IOException {
        ClusteringActionResponse result = new ClusteringActionRequestBuilder(client)
            .setQueryHint("data mining")
            .addSourceFieldMapping("title", LogicalField.TITLE)
            .addHighlightedFieldMapping("content", LogicalField.CONTENT)
            .setSearchRequest(
              client.prepareSearch()
                    .setIndices(INDEX_NAME)
                    .setTypes("test")
                    .setSize(100)
                    .setQuery(QueryBuilders.termQuery("_all", "data"))
                    .highlighter(new HighlightBuilder().preTags("").postTags(""))
                    .setFetchSource(new String[] {"title"}, null)
                    .highlighter(new HighlightBuilder().field("content")))
            .execute().actionGet();
    
        checkValid(result);
        checkJsonSerialization(result);
    }

    public void testAttributes() throws IOException {
        Map<String,Object> attrs = new HashMap<>();
        LingoClusteringAlgorithmDescriptor.attributeBuilder(attrs)
            .desiredClusterCountBase(5);

        ClusteringActionResponse result = new ClusteringActionRequestBuilder(client)
            .setQueryHint("data mining")
            .addSourceFieldMapping("title", LogicalField.TITLE)
            .addSourceFieldMapping("content", LogicalField.CONTENT)
            .addAttributes(attrs)
            .setSearchRequest(
              client.prepareSearch()
                    .setIndices(INDEX_NAME)
                    .setTypes("test")
                    .setSize(100)
                    .setQuery(QueryBuilders.termQuery("_all", "data"))
                    .setFetchSource(new String[] {"title", "content"}, null))
            .execute().actionGet();

        checkValid(result);
        checkJsonSerialization(result);
        
        Assertions.assertThat(result.getDocumentGroups())
            .hasSize(5 + /* other topics */ 1);
    }
    
    public void testLanguageField() throws IOException {
        Map<String,Object> attrs = new HashMap<>();

        // We can't serialize enum attributes via ES infrastructure so use string
        // constants from the descriptor.
        attrs.put(
                MultilingualClusteringDescriptor.Keys.LANGUAGE_AGGREGATION_STRATEGY,
                LanguageAggregationStrategy.FLATTEN_NONE.name());

        ClusteringActionResponse result = new ClusteringActionRequestBuilder(client)
            .setQueryHint("data mining")
            .addSourceFieldMapping("title", LogicalField.TITLE)
            .addSourceFieldMapping("content", LogicalField.CONTENT)
            .addSourceFieldMapping("rndlang", LogicalField.LANGUAGE)
            .addAttributes(attrs)
            .setSearchRequest(
              client.prepareSearch()
                    .setIndices(INDEX_NAME)
                    .setTypes("test")
                    .setSize(100)
                    .setQuery(QueryBuilders.termQuery("_all", "data"))
                    .setFetchSource(new String[] {"title", "content", "rndlang"}, null))
            .get();

        checkValid(result);
        checkJsonSerialization(result);

        // Top level groups should be input documents' languages (aggregation strategy above).
        DocumentGroup[] documentGroups = result.getDocumentGroups();
        Set<String> allLanguages = new HashSet<>();
        for (LanguageCode code : LanguageCode.values()) {
            allLanguages.add(code.toString());
        }

        for (DocumentGroup group : documentGroups) {
            if (!group.isOtherTopics()) {
                allLanguages.remove(group.getLabel());
            }
        }

        Assertions.assertThat(allLanguages.size())
            .describedAs("Expected a lot of languages to appear in top groups: " + allLanguages)
            .isLessThan(LanguageCode.values().length / 2);
    }
    
    public void testListAlgorithms() throws IOException {
        ListAlgorithmsActionResponse response = 
                new ListAlgorithmsActionRequestBuilder(client).get();

        List<String> algorithms = response.getAlgorithms();
        Assertions.assertThat(algorithms)
            .isNotEmpty()
            .contains("stc", "lingo", "kmeans");
    }

    public void testNonexistentFields() throws IOException {
        ClusteringActionResponse result = new ClusteringActionRequestBuilder(client)
            .setQueryHint("data mining")
            .addSourceFieldMapping("_nonexistent_", LogicalField.TITLE)
            .addSourceFieldMapping("_nonexistent_", LogicalField.CONTENT)
            .setSearchRequest(
              client.prepareSearch()
                    .setIndices(INDEX_NAME)
                    .setTypes("test")
                    .setSize(100)
                    .setQuery(QueryBuilders.termQuery("_all", "data"))
                    .setFetchSource(new String[] {"title", "content"}, null))
            .execute().actionGet();

        // There should be no clusters, but no errors.
        checkValid(result);
        checkJsonSerialization(result);

        // Top level groups should be input documents' languages (aggregation strategy above).
        DocumentGroup[] documentGroups = result.getDocumentGroups();
        for (DocumentGroup group : documentGroups) {
            if (!group.isOtherTopics()) {
                Assertions.fail("Expected no clusters for non-existent fields.");
            }
        }
    }
    
    public void testNonexistentAlgorithmId() throws IOException {
        // The query should result in an error.
        try {
            new ClusteringActionRequestBuilder(client)
                .setQueryHint("")
                .addSourceFieldMapping("_nonexistent_", LogicalField.TITLE)
                .setAlgorithm("_nonexistent_")
                .setSearchRequest(
                  client.prepareSearch()
                        .setIndices(INDEX_NAME)
                        .setTypes("test")
                        .setSize(100)
                        .setQuery(QueryBuilders.termQuery("_all", "data"))
                        .setFetchSource(new String[] {"title", "content"}, null))
                .execute().actionGet();
            throw Preconditions.unreachable();
        } catch (IllegalArgumentException e) {
            Assertions.assertThat(e)
                .hasMessageContaining("No such algorithm:");
        }
    }

    public void testPropagatingAlgorithmException() throws IOException {
        // The query should result in an error.
        try {
            Map<String,Object> attrs = new HashMap<>();
            // Out of allowed range (should cause an exception).
            STCClusteringAlgorithmDescriptor.attributeBuilder(attrs)
                .ignoreWordIfInHigherDocsPercent(Double.MAX_VALUE);

            new ClusteringActionRequestBuilder(client)
                .setQueryHint("")
                .addSourceFieldMapping("title", LogicalField.TITLE)
                .addSourceFieldMapping("content", LogicalField.CONTENT)
                .setAlgorithm("stc")
                .addAttributes(attrs)
                .setSearchRequest(
                  client.prepareSearch()
                        .setIndices(INDEX_NAME)
                        .setTypes("test")
                        .setSize(100)
                        .setQuery(QueryBuilders.termQuery("_all", "data"))
                        .setFetchSource(new String[] {"title", "content"}, null))
                .execute().actionGet();
            throw Preconditions.unreachable();
        } catch (ElasticsearchException e) {
            Assertions.assertThat(e)
                .hasMessageContaining("Search results clustering error:")
                .hasMessageContaining(STCClusteringAlgorithmDescriptor.Keys.IGNORE_WORD_IF_IN_HIGHER_DOCS_PERCENT);
        }
    }    

    public void testIncludeHits() throws IOException {
        // same search with and without hits
        SearchRequestBuilder req = client.prepareSearch()
                .setIndices(INDEX_NAME)
                .setTypes("test")
                .setSize(2)
                .setQuery(QueryBuilders.termQuery("_all", "data"))
                .setFetchSource(new String[] {"content"}, null);

        // with hits (default)
        ClusteringActionResponse resultWithHits = new ClusteringActionRequestBuilder(client)
            .setQueryHint("data mining")
            .setAlgorithm("stc")
            .addSourceFieldMapping("title", LogicalField.TITLE)
            .setSearchRequest(req)
            .execute().actionGet();
        checkValid(resultWithHits);
        checkJsonSerialization(resultWithHits);
        // get JSON output
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        resultWithHits.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        JSONObject jsonWithHits = new JSONObject(builder.string());
        Assertions.assertThat(jsonWithHits.has("hits")).isTrue();

        // without hits
        ClusteringActionResponse resultWithoutHits = new ClusteringActionRequestBuilder(client)
            .setQueryHint("data mining")
            .setMaxHits(0)
            .setAlgorithm("stc")
            .addSourceFieldMapping("title", LogicalField.TITLE)
            .setSearchRequest(req)
            .execute().actionGet();
        checkValid(resultWithoutHits);
        checkJsonSerialization(resultWithoutHits);

        // get JSON output
        builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        resultWithoutHits.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        JSONObject jsonWithoutHits = new JSONObject(builder.string());
        Assertions.assertThat(
                jsonWithoutHits
                    .getJSONObject("hits")
                    .getJSONArray("hits").length()).isEqualTo(0);

        // insert hits into jsonWithoutHits
        JSONObject jsonHits = (JSONObject)jsonWithHits.get("hits");
        jsonWithoutHits.put("hits", jsonHits);

        // took can vary, so ignore it
        jsonWithoutHits.remove("took");
        jsonWithHits.remove("took");

        // info can vary (clustering-millis, output_hits), so ignore it
        jsonWithoutHits.remove("info");
        jsonWithHits.remove("info");

        // profile can vary
        jsonWithoutHits.remove("profile");
        jsonWithHits.remove("profile");

        // now they should match
        LOGGER.debug("--> with:\n" + jsonWithHits.toString());
        LOGGER.debug("--> without:\n" + jsonWithoutHits.toString());
        Assertions.assertThat(jsonWithHits.toString()).isEqualTo(jsonWithoutHits.toString());
    }
    
    public void testMaxHits() throws IOException {
        // same search with and without hits
        SearchRequestBuilder req = client.prepareSearch()
                .setIndices(INDEX_NAME)
                .setTypes("test")
                .setSize(2)
                .setQuery(QueryBuilders.termQuery("_all", "data"))
                .setFetchSource(new String[] {"content"}, null);

        // Limit the set of hits to just top 2.
        ClusteringActionResponse limitedHits = new ClusteringActionRequestBuilder(client)
            .setQueryHint("data mining")
            .setMaxHits(2)
            .setAlgorithm("stc")
            .addSourceFieldMapping("title", LogicalField.TITLE)
            .setSearchRequest(req)
            .execute().actionGet();
        checkValid(limitedHits);
        checkJsonSerialization(limitedHits);

        Assertions.assertThat(limitedHits.getSearchResponse().getHits().hits())
            .hasSize(2);

        // get JSON output
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        limitedHits.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        JSONObject json = new JSONObject(builder.string());
        Assertions.assertThat(json
                    .getJSONObject("hits")
                    .getJSONArray("hits").length()).isEqualTo(2);
    }        
}
