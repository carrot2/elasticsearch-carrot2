package org.carrot2.elasticsearch.debug;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.apache.logging.log4j.Logger;
import org.carrot2.elasticsearch.ClusteringPlugin;
import org.carrot2.elasticsearch.ListAlgorithmsAction.ListAlgorithmsActionRequestBuilder;
import org.carrot2.elasticsearch.ListAlgorithmsAction.ListAlgorithmsActionResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * Starts a node locally and serves _site data (Eclipse, for debugging).
 * http://localhost:8983/_plugin/main
 */
public class CallAction {

    private static final Logger LOGGER = Loggers.getLogger(CallAction.class);

    public static void main(String[] args) throws Exception {
        Settings settings = Settings.builder().put("client.transport.sniff", true).build();
        TransportClient client = new PreBuiltTransportClient(settings, ClusteringPlugin.class);
        client.addTransportAddress(new TransportAddress(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300)));

        NodesInfoResponse r = client.admin().cluster().prepareNodesInfo().get();
        LOGGER.debug(r);

        LOGGER.debug("--");
        ListAlgorithmsActionResponse response = new ListAlgorithmsActionRequestBuilder(client).get();
        LOGGER.debug(response);
    }
}
