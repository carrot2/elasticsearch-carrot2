package org.carrot2.elasticsearch;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.elasticsearch.plugins.Plugin;
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
    protected Collection<Class<? extends Plugin>> nodePlugins() {
      return Arrays.<Class<? extends Plugin>> asList(ClusteringPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
      return nodePlugins();
    }

    public ListAlgorithmsActionRestIT(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws IOException, RestTestParseException {
        return ESRestTestCase.createParameters(0, 1);
    }
}
