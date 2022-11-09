package org.carrot2.elasticsearch;

import java.io.IOException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

/** Reusable stuff related to {@link Object#toString()} implementations. */
final class ToString {
  public static String objectToJson(ToXContent xcontentObject) {
    try {
      XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
      builder.startObject();
      xcontentObject.toXContent(builder, ToXContent.EMPTY_PARAMS);
      builder.endObject();
      return Strings.toString(builder);
    } catch (IOException e) {
      try {
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        builder.field("error", e.getMessage());
        builder.field("class", e.getClass().getName());
        builder.endObject();
        return Strings.toString(builder);
      } catch (IOException e2) {
        return "{ \"error\": \"Could not serialize the underlying error.\"}";
      }
    }
  }
}
