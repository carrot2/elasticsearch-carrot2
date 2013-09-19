package org.carrot2.elasticsearch.debug;

import java.io.ByteArrayOutputStream;

import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import com.google.common.io.Resources;

public class XContentTests {
    public static void main(String[] args) throws Exception {
        byte[] jsonResource = Resources.toByteArray(
                Resources.getResource(XContentTests.class, "post_cluster_by_url.json"));

        XContent xcontent = XContentFactory.xContent(jsonResource);
        XContentType type = XContentType.YAML; // xcontent.type();
        XContentParser parser = xcontent.createParser(jsonResource);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XContentBuilder builder = XContentFactory.contentBuilder(type, baos).copyCurrentStructure(parser);
        builder.close();

        System.out.println("Out:");
        System.out.println(new String(baos.toByteArray()));
    }
}
