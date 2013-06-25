package org.carrot2.elasticsearch.plugin;

import static org.elasticsearch.common.collect.Lists.newArrayList;

import java.util.Collection;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.AbstractPlugin;

/**
 * 
 */
public class Carrot2Plugin extends AbstractPlugin {
    private final Settings settings;

    public Carrot2Plugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public String name() {
        return "misc-carrot2";
    }

    @Override
    public String description() {
        return "Provides search results clustering via the Carrot2 framework";
    }

    public void onModule(ActionModule actionModule) {
        actionModule.registerAction(
                Carrot2ClusteringAction.INSTANCE, 
                TransportCarrot2ClusteringAction.class);
    } 

    @Override
    public Collection<Class<? extends Module>> modules() {
        Collection<Class<? extends Module>> modules = newArrayList();
        if (settings.getAsBoolean("carrot2.enabled", true)) {
            modules.add(Carrot2Module.class);
        }
        return modules;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = newArrayList();
        if (settings.getAsBoolean("carrot2.enabled", true)) {
            services.add(ControllerSingleton.class);
        }
        return services;
    }
}
