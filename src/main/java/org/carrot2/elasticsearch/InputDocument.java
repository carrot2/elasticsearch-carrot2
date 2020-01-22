package org.carrot2.elasticsearch;

import org.carrot2.clustering.Document;

import java.util.function.BiConsumer;

public class InputDocument implements Document {
   private final int id;
   private final String title;
   private final String content;
   private final String langCode;
   private final String url;
   private final String hitId;

   public InputDocument(String title, String content, String url, String langCode, String hitId, int id) {
      this.title = title;
      this.content = content;
      this.url = url;
      this.langCode = langCode;
      this.hitId = hitId;
      this.id = id;
   }

   @Override
   public void visitFields(BiConsumer<String, String> fieldConsumer) {
      fieldConsumer.accept("title", title);
      fieldConsumer.accept("content", content);
   }

   public String getStringId() {
      return String.valueOf(id);
   }
}
