package org.carrot2.elasticsearch.plugin;

import static org.elasticsearch.common.collect.Lists.newArrayList;

import java.util.Collection;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;

/** */
public class Carrot2Plugin extends AbstractPlugin {
    private final boolean moduleEnabled;

    public Carrot2Plugin(Settings settings) {
        this.moduleEnabled = settings.getAsBoolean("carrot2.enabled", true);
    }

    @Override
    public String name() {
        return "misc-carrot2";
    }

    @Override
    public String description() {
        return "Provides search results clustering via the Carrot2 framework";
    }

    /* Invoked on component assembly. */
    public void onModule(ActionModule actionModule) {
        if (moduleEnabled) {
            actionModule.registerAction(
                    Carrot2ClusteringAction.INSTANCE, 
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
            modules.add(Carrot2Module.class);
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
