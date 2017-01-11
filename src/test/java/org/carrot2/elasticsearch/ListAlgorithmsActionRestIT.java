package org.carrot2.elasticsearch;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;
import org.elasticsearch.test.rest.yaml.parser.ClientYamlTestParseException;

import java.io.IOException;

/**
 * REST API tests for {@link ListAlgorithmsAction}.
 */
public class ListAlgorithmsActionRestIT extends ESClientYamlSuiteTestCase {

    public ListAlgorithmsActionRestIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws IOException, ClientYamlTestParseException {
        return createParameters();
    }
}
