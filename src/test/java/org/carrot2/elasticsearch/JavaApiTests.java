package org.carrot2.elasticsearch;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.carrot2.clustering.lingo.LingoClusteringAlgorithmDescriptor;
import org.carrot2.core.LanguageCode;
import org.carrot2.text.clustering.MultilingualClustering.LanguageAggregationStrategy;
import org.carrot2.text.clustering.MultilingualClusteringDescriptor;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.index.query.QueryBuilders;
import org.fest.assertions.api.Assertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.Maps;

/**
 * Java API tests.
 */
public class JavaApiTests extends AbstractApiTest {
    @DataProvider(name = "clients")
    public static Object[][] clientProvider() {
        return new Object[][] {
                {localClient},
                {transportClient},
        };
    }

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
            .execute().actionGet();

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
}


