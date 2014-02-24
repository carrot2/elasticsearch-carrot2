package org.carrot2.elasticsearch;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.fest.assertions.api.Assertions;
import org.json.JSONObject;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

/**
 * Java API tests for {@link ClusteringAction}.
 */
public class ClusteringActionTests extends AbstractApiTest {
    @Test(dataProvider = "clients")
    public void testComplexQuery(Client client) throws IOException {
        ClusteringActionResponse result = new ClusteringActionRequestBuilder(client)
            .setQueryHint("data mining")
            .addFieldMapping("title", LogicalField.TITLE)
            .addHighlightedFieldMapping("content", LogicalField.CONTENT)
            .setSearchRequest(
              client.prepareSearch()
                    .setIndices(INDEX_NAME)
                    .setTypes("test")
                    .setSize(100)
                    .setQuery(QueryBuilders.termQuery("_all", "data"))
                    .setHighlighterPreTags("")
                    .setHighlighterPostTags("")
                    .addField("title")
                    .addHighlightedField("content"))
            .execute().actionGet();
    
        checkValid(result);
        checkJsonSerialization(result);
    }
    
    @Test(dataProvider = "clients")
    public void testAttributes(Client client) throws IOException {
        Map<String,Object> attrs = Maps.newHashMap();
        LingoClusteringAlgorithmDescriptor.attributeBuilder(attrs)
            .desiredClusterCountBase(5);

        ClusteringActionResponse result = new ClusteringActionRequestBuilder(client)
            .setQueryHint("data mining")
            .addFieldMapping("title", LogicalField.TITLE)
            .addFieldMapping("content", LogicalField.CONTENT)
            .addAttributes(attrs)
            .setSearchRequest(
              client.prepareSearch()
                    .setIndices(INDEX_NAME)
                    .setTypes("test")
                    .setSize(100)
                    .setQuery(QueryBuilders.termQuery("_all", "data"))
                    .addFields("title", "content"))
            .execute().actionGet();

        checkValid(result);
        checkJsonSerialization(result);
        
        Assertions.assertThat(result.getDocumentGroups())
            .hasSize(5 + /* other topics */ 1);
    }

    @Test(dataProvider = "clients")
    public void testLanguageField(Client client) throws IOException {
        Map<String,Object> attrs = Maps.newHashMap();

        // We can't serialize enum attributes via ES infrastructure so use string
        // constants from the descriptor.
        attrs.put(
                MultilingualClusteringDescriptor.Keys.LANGUAGE_AGGREGATION_STRATEGY,
                LanguageAggregationStrategy.FLATTEN_NONE.name());

        ClusteringActionResponse result = new ClusteringActionRequestBuilder(client)
            .setQueryHint("data mining")
            .addFieldMapping("title", LogicalField.TITLE)
            .addFieldMapping("content", LogicalField.CONTENT)
            .addFieldMapping("rndlang", LogicalField.LANGUAGE)
            .addAttributes(attrs)
            .setSearchRequest(
              client.prepareSearch()
                    .setIndices(INDEX_NAME)
                    .setTypes("test")
                    .setSize(100)
                    .setQuery(QueryBuilders.termQuery("_all", "data"))
                    .addFields("title", "content", "rndlang"))
            .get();

        checkValid(result);
        checkJsonSerialization(result);

        // Top level groups should be input documents' languages (aggregation strategy above).
        DocumentGroup[] documentGroups = result.getDocumentGroups();
        Set<String> allLanguages = Sets.newHashSet();
        for (LanguageCode code : LanguageCode.values()) {
            allLanguages.add(code.toString());
        }

        for (DocumentGroup group : documentGroups) {
            if (!group.isOtherTopics()) {
                allLanguages.remove(group.getLabel());
            }
        }

        Assertions.assertThat(allLanguages.size())
            .describedAs("Expected a lot of languages to appear in top groups.")
            .isLessThan(LanguageCode.values().length / 2);
    }
    
    @Test(dataProvider = "clients")
    public void testListAlgorithms(Client client) throws IOException {
        ListAlgorithmsActionResponse response = 
                new ListAlgorithmsActionRequestBuilder(client).get();
        
        List<String> algorithms = response.getAlgorithms();
        Assertions.assertThat(algorithms)
            .isNotEmpty()
            .contains("stc", "lingo", "kmeans");
    }

    @Test(dataProvider = "clients")
    public void testNonexistentFields(Client client) throws IOException {
        ClusteringActionResponse result = new ClusteringActionRequestBuilder(client)
            .setQueryHint("data mining")
            .addFieldMapping("_nonexistent_", LogicalField.TITLE)
            .addFieldMapping("_nonexistent_", LogicalField.CONTENT)
            .setSearchRequest(
              client.prepareSearch()
                    .setIndices(INDEX_NAME)
                    .setTypes("test")
                    .setSize(100)
                    .setQuery(QueryBuilders.termQuery("_all", "data"))
                    .addFields("title", "content"))
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
    
    @Test(dataProvider = "clients")
    public void testNonexistentAlgorithmId(Client client) throws IOException {
        // The query should result in an error.
        try {
            new ClusteringActionRequestBuilder(client)
                .setQueryHint("")
                .addFieldMapping("_nonexistent_", LogicalField.TITLE)
                .setAlgorithm("_nonexistent_")
                .setSearchRequest(
                  client.prepareSearch()
                        .setIndices(INDEX_NAME)
                        .setTypes("test")
                        .setSize(100)
                        .setQuery(QueryBuilders.termQuery("_all", "data"))
                        .addFields("title", "content"))
                .execute().actionGet();
            throw Preconditions.unreachable();
        } catch (ElasticsearchException e) {
            Assertions.assertThat(e)
                .hasMessageContaining("No such algorithm:");
        }
    }

    @Test(dataProvider = "clients")
    public void testInvalidSearchQuery(Client client) throws IOException {
        // The query should result in an error.
        try {
            new ClusteringActionRequestBuilder(client)
                .setQueryHint("")
                .addFieldMapping("_nonexistent_", LogicalField.TITLE)
                .setAlgorithm("_nonexistent_")
                .setSearchRequest(
                  client.prepareSearch()
                        .setExtraSource("{ invalid json; [}")
                        .setIndices(INDEX_NAME)
                        .setTypes("test")
                        .setSize(100)
                        .setQuery(QueryBuilders.termQuery("_all", "data"))
                        .addFields("title", "content"))
                .execute().actionGet();
            throw Preconditions.unreachable();
        } catch (SearchPhaseExecutionException e) {
            ShardSearchFailure[] shardFailures = e.shardFailures();
            Assertions.assertThat(shardFailures).isNotEmpty();
            Assertions.assertThat(shardFailures[0].reason())
                .contains("Parse Failure");
        }
    }

    @Test(dataProvider = "clients")
    public void testPropagatingAlgorithmException(Client client) throws IOException {
        // The query should result in an error.
        try {
            Map<String,Object> attrs = Maps.newHashMap();
            // Out of allowed range (should cause an exception).
            STCClusteringAlgorithmDescriptor.attributeBuilder(attrs)
                .ignoreWordIfInHigherDocsPercent(Double.MAX_VALUE);

            new ClusteringActionRequestBuilder(client)
                .setQueryHint("")
                .addFieldMapping("title", LogicalField.TITLE)
                .addFieldMapping("content", LogicalField.CONTENT)
                .setAlgorithm("stc")
                .addAttributes(attrs)
                .setSearchRequest(
                  client.prepareSearch()
                        .setIndices(INDEX_NAME)
                        .setTypes("test")
                        .setSize(100)
                        .setQuery(QueryBuilders.termQuery("_all", "data"))
                        .addFields("title", "content"))
                .execute().actionGet();
            throw Preconditions.unreachable();
        } catch (ElasticsearchException e) {
            Assertions.assertThat(e)
                .hasMessageContaining("Search results clustering error:")
                .hasMessageContaining(STCClusteringAlgorithmDescriptor.Keys.IGNORE_WORD_IF_IN_HIGHER_DOCS_PERCENT);
        }
    }    

    @Test(dataProvider = "clients")
    public void testIncludeHits(Client client) throws IOException {
        // same search with and without hits
        SearchRequestBuilder req = client.prepareSearch()
                .setIndices(INDEX_NAME)
                .setTypes("test")
                .setSize(2)
                .setQuery(QueryBuilders.termQuery("_all", "data"))
                .addField("content");

        // with hits (default)
        ClusteringActionResponse resultWithHits = new ClusteringActionRequestBuilder(client)
            .setQueryHint("data mining")
            .setAlgorithm("stc")
            .addFieldMapping("title", LogicalField.TITLE)
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
            .setIncludeHits("false")
            .setAlgorithm("stc")
            .addFieldMapping("title", LogicalField.TITLE)
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
        Assertions.assertThat(jsonWithoutHits.has("hits")).isFalse();

        // insert hits into jsonWithoutHits
        JSONObject jsonHits = (JSONObject)jsonWithHits.get("hits");
        jsonWithoutHits.put("hits", jsonHits);
        // took can vary, so ignore it
        jsonWithoutHits.remove("took");
        jsonWithHits.remove("took");
        // info can vary (clustering-millis, output_hits), so ignore it
        jsonWithoutHits.remove("info");
        jsonWithHits.remove("info");
        // now they should match
        Assertions.assertThat(jsonWithHits.toString()).isEqualTo(jsonWithoutHits.toString());
    }
}


