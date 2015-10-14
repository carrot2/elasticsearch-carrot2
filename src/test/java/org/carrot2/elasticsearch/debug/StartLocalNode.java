package org.carrot2.elasticsearch.debug;

import static org.elasticsearch.common.settings.Settings.*;
import static org.elasticsearch.node.NodeBuilder.*;

import java.util.concurrent.CountDownLatch;

import org.carrot2.elasticsearch.ClusteringPlugin;
import org.elasticsearch.node.Node;

/**
 * Starts a node locally and serves _site data (Eclipse, for debugging).
 * http://localhost:8983/_plugin/main
 */
public class StartLocalNode {
    public static void main(String[] args) throws Exception {
        System.setProperty("es.logger.level", "TRACE");
        System.setProperty("es.logger.prefix", "");
        Node node = nodeBuilder().settings(settingsBuilder()
                .extendArray("plugin.types", ClusteringPlugin.class.getName())
                .put("path.home", "target/home")
                .put("path.data", "target/data")
                .put("path.plugins", "src")
                .put("cluster.name", "test-cluster"))
                .local(false)
                .node();

        node.start();

        new CountDownLatch(1).await();
    }
}
