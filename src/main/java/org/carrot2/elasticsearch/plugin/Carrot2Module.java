package org.carrot2.elasticsearch.plugin;

import org.elasticsearch.common.inject.AbstractModule;

/**
 * 
 */
public class Carrot2Module extends AbstractModule {
    @Override
    protected void configure() {
        bind(ControllerSingleton.class).asEagerSingleton();
    }
}
