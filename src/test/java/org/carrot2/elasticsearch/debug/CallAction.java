package org.carrot2.elasticsearch.debug;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.carrot2.elasticsearch.ClusteringPlugin;
import org.carrot2.elasticsearch.ListAlgorithmsAction.ListAlgorithmsActionRequestBuilder;
import org.carrot2.elasticsearch.ListAlgorithmsAction.ListAlgorithmsActionResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Starts a node locally and serves _site data (Eclipse, for debugging).
 * http://localhost:8983/_plugin/main
 */
public class CallAction {
    public static void main(String[] args) throws Exception {
        TransportClient client = TransportClient.builder()
                .settings(Settings.builder()
                        .put("plugin.types", ClusteringPlugin.class.getName())
                        .put("client.transport.sniff", true))
                .build();
        client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300)));

        NodesInfoResponse r = client.admin().cluster().prepareNodesInfo().get();
        System.out.println(r);
        
        System.out.println("--");
        ListAlgorithmsActionResponse response = new ListAlgorithmsActionRequestBuilder(client).get();
        System.out.println(response);
    }
}
