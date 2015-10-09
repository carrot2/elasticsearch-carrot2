package org.carrot2.elasticsearch;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.carrot2.elasticsearch.ClusteringAction.RestClusteringAction;
import org.carrot2.elasticsearch.ClusteringAction.TransportClusteringAction;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestModule;

/** */
public class ClusteringPlugin extends Plugin {
    /**
     * Master on/off switch property for the plugin (general settings).
     */
    public static final String DEFAULT_ENABLED_PROPERTY_NAME = "carrot2.enabled";

    /**
     * Plugin's main configuration file (under ES/conf).
     */
    public static final String PLUGIN_CONFIG_FILE_NAME = "carrot2";

    /**
     * A property key in {@link #PLUGIN_CONFIG_FILE_NAME} holding
     * the default component suite's resource name.
     */
    public static final String DEFAULT_SUITE_PROPERTY_NAME = "suite";

    /**
     * The default suite resource. 
     * @see #DEFAULT_SUITE_PROPERTY_NAME
     */
    public static final String DEFAULT_SUITE_RESOURCE = "carrot2.suite.xml";

    /**
     * A property key in {@link #PLUGIN_CONFIG_FILE_NAME} holding
     * the default location of additional resources (stopwords, etc.) for
     * algorithms. The location is resolved relative to <code>es/conf</code>
     * but can be absolute. By default it is <code>.</code>.
     */
    public static final String DEFAULT_RESOURCES_PROPERTY_NAME = "resources";

    /**
     * A property key in {@link #PLUGIN_CONFIG_FILE_NAME} with the size
     * of the clustering controller's algorithm pool. By default the size
     * is zero, meaning the pool is sized dynamically. You can specify a fixed
     * number of component instances to limit resource usage. 
     */
    public static final String DEFAULT_COMPONENT_SIZE_PROPERTY_NAME = "controller.pool-size";

    private final boolean pluginEnabled;
    private final ESLogger logger;

    public ClusteringPlugin(Settings settings) {
        this.pluginEnabled = settings.getAsBoolean(DEFAULT_ENABLED_PROPERTY_NAME, true);
        this.logger = Loggers.getLogger("plugin.carrot2", settings);
    }

    @Override
    public String name() {
        return "clustering-carrot2";
    }

    @Override
    public String description() {
        return "Provides search results clustering via the Carrot2 framework";
    }

    /* Invoked on component assembly. */
    public void onModule(ActionModule actionModule) {
        if (pluginEnabled) {
            actionModule.registerAction(
                    ClusteringAction.INSTANCE, 
                    TransportClusteringAction.class);
            actionModule.registerAction(
                    ListAlgorithmsAction.INSTANCE, 
                    ListAlgorithmsAction.TransportListAlgorithmsAction.class);
        }
    }

    /* Invoked on component assembly. */
    public void onModule(RestModule restModule) {
        if (pluginEnabled) {
            restModule.addRestAction(RestClusteringAction.class);
            restModule.addRestAction(ListAlgorithmsAction.RestListAlgorithmsAction.class);
        }
    }
    
    @Override
    public Collection<Module> nodeModules() {
        if (pluginEnabled) {
            return Arrays.<Module> asList(new ClusteringModule());
        } else {
            return Collections.emptyList();
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (pluginEnabled) {
            return Arrays.<Class<? extends LifecycleComponent>> asList(
                    ControllerSingleton.class);
        } else {
            logger.info("Plugin disabled.", name());
            return Collections.emptyList();
        }
    }
}
