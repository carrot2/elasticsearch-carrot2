package org.carrot2.elasticsearch;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

/**
 * Reusable stuff related to {@link Object#toString()} implementations.
 */
final class ToString {
    /* */
    public static String objectToJson(ToXContent xcontentObject) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            xcontentObject.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
                builder.startObject();
                builder.field("error", e.getMessage());
                builder.field("class", e.getClass().getName());
                builder.endObject();
                return builder.string();
            } catch (IOException e2) {
                return "{ \"error\": \"Could not serialize the underlying error.\"}";
            }
        }
    }
}
