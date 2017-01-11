package org.carrot2.elasticsearch;

import org.carrot2.elasticsearch.ClusteringAction.RestClusteringAction;
import org.carrot2.elasticsearch.ClusteringAction.TransportClusteringAction;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** */
public class ClusteringPlugin extends Plugin implements ActionPlugin {
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
     * the default component suite's resource name.
     */
    public static final String DEFAULT_SUITE_PROPERTY_NAME = "suite";

    /**
     * A property key holding
     * the default location of additional resources (stopwords, etc.) for
     * algorithms. The location is resolved relative to <code>es/conf</code>
     * but can be absolute. By default it is <code>.</code>.
     */
    public static final String DEFAULT_RESOURCES_PROPERTY_NAME = "resources";

    /**
     * A property key with the size
     * of the clustering controller's algorithm pool. By default the size
     * is zero, meaning the pool is sized dynamically. You can specify a fixed
     * number of component instances to limit resource usage.
     */
    public static final String DEFAULT_COMPONENT_SIZE_PROPERTY_NAME = "controller.pool-size";

    private final boolean transportClient;
    private final boolean pluginEnabled;

    public ClusteringPlugin(Settings settings) {
        this.pluginEnabled = settings.getAsBoolean(DEFAULT_ENABLED_PROPERTY_NAME, true);
        this.transportClient = TransportClient.CLIENT_TYPE.equals(Client.CLIENT_TYPE_SETTING_S.get(settings));
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
    public List<Class<? extends RestHandler>> getRestHandlers() {
        if (pluginEnabled) {
            return Arrays.asList(RestClusteringAction.class, ListAlgorithmsAction.RestListAlgorithmsAction.class);
        }
        return Collections.emptyList();
    }

    @Override
    public Collection<Module> createGuiceModules() {
        if (pluginEnabled && !transportClient) {
            return Collections.singletonList(new ClusteringModule());
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        if (pluginEnabled) {
            if (!transportClient) {
                return Collections.singletonList(ControllerSingleton.class);
            }
        }
        return Collections.emptyList();
    }
}
