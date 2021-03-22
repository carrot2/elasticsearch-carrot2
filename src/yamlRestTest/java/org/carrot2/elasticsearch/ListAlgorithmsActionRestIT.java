
package org.carrot2.elasticsearch;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;

/** REST API tests for {@code ListAlgorithmsAction}. */
public class ListAlgorithmsActionRestIT extends ESClientYamlSuiteTestCase {

  public ListAlgorithmsActionRestIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
    super(testCandidate);
  }

  @ParametersFactory
  public static Iterable<Object[]> parameters() throws Exception {
    return createParameters();
  }
}
