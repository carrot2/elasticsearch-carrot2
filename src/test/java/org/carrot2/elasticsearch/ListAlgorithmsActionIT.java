package org.carrot2.elasticsearch;

import org.assertj.core.api.Assertions;
import org.carrot2.elasticsearch.ListAlgorithmsAction.ListAlgorithmsActionRequestBuilder;
import org.carrot2.elasticsearch.ListAlgorithmsAction.ListAlgorithmsActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE; 

@ClusterScope(scope = SUITE, transportClientRatio = 0) 
public class ListAlgorithmsActionIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        System.out.println("## Invoked nodeSettings()");
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", ClusteringPlugin.class.getName())
                .build();
    }
    
    @Override
    protected Settings transportClientSettings() {
        System.out.println("## Invoked transportClientSettings()");
        return Settings.builder()
                .put(super.transportClientSettings())
                .put("plugin.types", ClusteringPlugin.class.getName())
                .build();
    }

    public void testAlgorithmsAreListed() throws Exception {
        Client client = client();
        System.out.println("## test(): " + client + " " + client.settings().toDelimitedString('\n'));
        ListAlgorithmsActionResponse response = new ListAlgorithmsActionRequestBuilder(client).get();
        Assertions.assertThat(response.getAlgorithms())
          .describedAs("A list of algorithms")
          .contains("stc", "lingo", "kmeans");  
    }
}
