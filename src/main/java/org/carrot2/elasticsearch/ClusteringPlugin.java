package org.carrot2.elasticsearch;

import static org.elasticsearch.common.collect.Lists.newArrayList;

import java.util.Collection;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

/** */
public class ClusteringPlugin extends AbstractPlugin {
    public static final String DEFAULT_CONFIG_FILE = "carrot2";
    public static final String DEFAULT_SUITE_PROPERTY_NAME = "suite";
    public static final String DEFAULT_ENABLED_PROPERTY_NAME = "carrot2.enabled";

    private final boolean moduleEnabled;

    public ClusteringPlugin(Settings settings) {
        this.moduleEnabled = settings.getAsBoolean(DEFAULT_ENABLED_PROPERTY_NAME, true);
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
        if (moduleEnabled) {
            actionModule.registerAction(
                    ClusteringAction.INSTANCE, 
                    TransportCarrot2ClusteringAction.class);
        }
    } 

    /* Invoked on component assembly. */
    public void onModule(RestModule restModule) {
        if (moduleEnabled) {
            restModule.addRestAction(RestCarrot2ClusteringAction.class);
        }
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        if (moduleEnabled) {
            modules.add(ClusteringModule.class);
        }
        return modules;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = newArrayList();
        if (moduleEnabled) {
            services.add(ControllerSingleton.class);
        }
        return services;
    }
}
