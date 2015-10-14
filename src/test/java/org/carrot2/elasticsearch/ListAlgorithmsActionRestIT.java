package org.carrot2.elasticsearch;

import java.io.IOException;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.test.rest.parser.RestTestParseException;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

/**
 * REST API tests for {@link ListAlgorithmsAction}.
 */
public class ListAlgorithmsActionRestIT extends ESRestTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", ClusteringPlugin.class.getName())
                .build();
    }
    
    @Override
    protected Settings transportClientSettings() {
        return Settings.builder()
                .put(super.transportClientSettings())
                .put("plugin.types", ClusteringPlugin.class.getName())
                .build();
    }
    
    @Override
    protected Settings externalClusterClientSettings() {
        return Settings.builder()
                .put(super.externalClusterClientSettings())
                .put("plugin.types", ClusteringPlugin.class.getName())
                .build();
    }

    public ListAlgorithmsActionRestIT(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws IOException, RestTestParseException {
        return ESRestTestCase.createParameters(0, 1);
    }
}
