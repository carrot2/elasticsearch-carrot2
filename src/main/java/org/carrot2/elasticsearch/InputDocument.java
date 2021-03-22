
package org.carrot2.elasticsearch;

import java.util.Objects;
import java.util.function.BiConsumer;
import org.carrot2.clustering.Document;

public class InputDocument implements Document {
  private final String title;
  private final String content;
  private final String language;
  private final String hitId;

  public InputDocument(String title, String content, String language, String hitId) {
    this.title = title;
    this.content = content;
    this.language = language;
    this.hitId = Objects.requireNonNull(hitId);
  }

  @Override
  public void visitFields(BiConsumer<String, String> fieldConsumer) {
    fieldConsumer.accept("title", title);
    fieldConsumer.accept("content", content);
  }

  public String getStringId() {
    return hitId;
  }

  public String language() {
    return language;
  }
}
