package org.carrot2.elasticsearch;

import org.assertj.core.api.Assertions;
import org.carrot2.elasticsearch.ListAlgorithmsAction.ListAlgorithmsActionRequestBuilder;
import org.carrot2.elasticsearch.ListAlgorithmsAction.ListAlgorithmsActionResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

public class ListAlgorithmsActionIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", ClusteringPlugin.class.getName())
                .build();
    }

    public void testAlgorithmsAreListed() throws Exception {
        ListAlgorithmsActionResponse response = new ListAlgorithmsActionRequestBuilder(client()).get();
        Assertions.assertThat(response.getAlgorithms())
          .describedAs("A list of algorithms")
          .contains("stc", "lingo", "kmeans");  
    }
}
