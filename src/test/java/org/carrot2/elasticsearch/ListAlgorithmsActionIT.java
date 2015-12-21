package org.carrot2.elasticsearch;

import static org.elasticsearch.test.ESIntegTestCase.Scope.*;

import java.util.Arrays;
import java.util.Collection;

import org.assertj.core.api.Assertions;
import org.carrot2.elasticsearch.ListAlgorithmsAction.ListAlgorithmsActionRequestBuilder;
import org.carrot2.elasticsearch.ListAlgorithmsAction.ListAlgorithmsActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope; 

@ClusterScope(scope = SUITE, transportClientRatio = 0) 
public class ListAlgorithmsActionIT extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
      return Arrays.<Class<? extends Plugin>> asList(ClusteringPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
      return nodePlugins();
    }

    public void testAlgorithmsAreListed() throws Exception {
        Client client = client();
        
        ListAlgorithmsActionResponse response = new ListAlgorithmsActionRequestBuilder(client).get();
        Assertions.assertThat(response.getAlgorithms())
          .describedAs("A list of algorithms")
          .contains("stc", "lingo", "kmeans");
    }
}
