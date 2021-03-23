
package org.carrot2.elasticsearch;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import org.carrot2.clustering.Document;

/**
 * An implementation of {@link org.carrot2.clustering.Document} that stores explicit key-value
 * fields.
 */
class FieldMapDocument implements Document {
  private final LinkedHashMap<String, String> fields;

  FieldMapDocument(Map<String, String> fields) {
    this.fields = new LinkedHashMap<>(fields);
  }

  @Override
  public void visitFields(BiConsumer<String, String> fieldConsumer) {
    fields.forEach(fieldConsumer);
  }
}
