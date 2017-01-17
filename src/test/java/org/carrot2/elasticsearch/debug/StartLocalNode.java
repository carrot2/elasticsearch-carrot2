package org.carrot2.elasticsearch.debug;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import org.carrot2.elasticsearch.ClusteringPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;

/**
 * Starts a node locally and serves _site data (Eclipse, for debugging).
 * http://localhost:8983/_plugin/main
 */
public class StartLocalNode {
    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder()
            .put("cluster.name", "test-cluster")
            .put("path.home", "target/home")
            .put("path.data", "target/data")
            .put("path.plugins", "src")            
            .build();
        Collection<Class<? extends Plugin>> plugins = 
            Collections.<Class<? extends Plugin>> singletonList(ClusteringPlugin.class);
        try (Node node = new MockNode(settings, plugins)) {
          node.start();
          new CountDownLatch(1).await();
        }
    }
}
