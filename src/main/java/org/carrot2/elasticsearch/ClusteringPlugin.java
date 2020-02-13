package org.carrot2.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.carrot2.elasticsearch.ClusteringAction.TransportClusteringAction;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public class ClusteringPlugin extends Plugin implements ExtensiblePlugin, ActionPlugin {
    /**
     * Master on/off switch property for the plugin (general settings).
     */
    public static final String DEFAULT_ENABLED_PROPERTY_NAME = "carrot2.enabled";

    /**
     * Plugin name.
     */
    public static final String PLUGIN_NAME = "elasticsearch-carrot2";

    /**
     * A property key holding
     * the default location of additional resources (stopwords, etc.) for
     * algorithms. The location is resolved relative to <code>es/conf</code>
     * but can be absolute. By default it is <code>.</code>.
     */
    public static final String DEFAULT_RESOURCES_PROPERTY_NAME = "resources";

    private final boolean transportClient;
    private final boolean pluginEnabled;
    private final Logger logger;

    public ClusteringPlugin(Settings settings) {
        this.pluginEnabled = settings.getAsBoolean(DEFAULT_ENABLED_PROPERTY_NAME, true);
        this.transportClient = TransportClient.CLIENT_TYPE.equals(Client.CLIENT_TYPE_SETTING_S.get(settings));
        this.logger = LogManager.getLogger("plugin.carrot2");
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (pluginEnabled) {
            return Arrays.asList(
                    new ActionHandler<>(ClusteringAction.INSTANCE, TransportClusteringAction.class),
                    new ActionHandler<>(ListAlgorithmsAction.INSTANCE, ListAlgorithmsAction.TransportListAlgorithmsAction.class));
        }
        return Collections.emptyList();
    }

    @Override
    public List<RestHandler> getRestHandlers(Settings settings, RestController restController,
      ClusterSettings clusterSettings, IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
      IndexNameExpressionResolver indexNameExpressionResolver, Supplier<DiscoveryNodes> nodesInCluster) {
    return Arrays.asList(
        new ClusteringAction.RestClusteringAction(restController),
        new ListAlgorithmsAction.RestListAlgorithmsAction(restController));
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService,
                                               ThreadPool threadPool, ResourceWatcherService resourceWatcherService,
                                               ScriptService scriptService, NamedXContentRegistry xContentRegistry,
                                               Environment environment, NodeEnvironment nodeEnvironment,
                                               NamedWriteableRegistry namedWriteableRegistry) {
        List<Object> components = new ArrayList<>();
        if (pluginEnabled && !transportClient) {
            components.add(new ClusteringContext(environment));
        }
        return components;
    }
}
