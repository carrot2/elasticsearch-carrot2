package org.carrot2.elasticsearch;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;

import com.google.common.io.Resources;

public class XContentTests {
    public static void main(String[] args) throws Exception {
        byte[] jsonResource = Resources.toByteArray(
                Resources.getResource(XContentTests.class, "post_cluster_by_url.json"));

        XContentParser parser = XContentFactory.xContent(jsonResource).createParser(jsonResource);
        Map<String, Object> mapOrderedAndClose = parser.mapOrderedAndClose();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XContentFactory.smileBuilder(baos).map(mapOrderedAndClose).close();
        
        System.out.println("Out:");
        System.out.println(new String(baos.toByteArray()));
    }
}
