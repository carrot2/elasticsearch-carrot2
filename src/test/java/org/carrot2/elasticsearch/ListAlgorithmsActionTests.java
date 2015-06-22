package org.carrot2.elasticsearch;

import java.io.IOException;

import org.carrot2.elasticsearch.ListAlgorithmsAction.ListAlgorithmsActionRequestBuilder;
import org.carrot2.elasticsearch.ListAlgorithmsAction.ListAlgorithmsActionResponse;
import org.elasticsearch.client.Client;
import org.assertj.core.api.Assertions;
import org.testng.annotations.Test;

/**
 * Java API tests for {@link ListAlgorithmsAction}.
 */
public class ListAlgorithmsActionTests extends AbstractApiTest {
    @Test(dataProvider = "clients")
    public void testListAlgorithms(Client client) throws IOException {
        ListAlgorithmsActionResponse response = new ListAlgorithmsActionRequestBuilder(client).get();
        Assertions.assertThat(response.getAlgorithms())
            .describedAs("A list of algorithms")
            .contains("stc", "lingo", "kmeans");            
    }
}
