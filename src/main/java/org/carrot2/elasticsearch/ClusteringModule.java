package org.carrot2.elasticsearch;

import org.elasticsearch.common.inject.AbstractModule;

public class ClusteringModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(ClusteringContext.class).asEagerSingleton();
    }
}
