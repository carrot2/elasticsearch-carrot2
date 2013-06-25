package org.carrot2.elasticsearch.plugin;

import java.util.Map;

import org.carrot2.core.Controller;
import org.carrot2.core.ControllerFactory;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.testng.collections.Maps;

/**
 * Holds the {@link Controller} singleton initialized and ready throughout
 * the {@link Node}'s lifecycle.
 */
public class ControllerSingleton extends AbstractLifecycleComponent<ControllerSingleton> {
    public static final String CARROT2_PREFIX = "carrot2";
    private Controller controller;

    @Inject
    protected ControllerSingleton(Settings settings) {
        super(settings);
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        try {
            controller = ControllerFactory.createPooling();

            Map<String, Object> c2SettingsAsMap = Maps.newHashMap();
            c2SettingsAsMap.putAll(settings.getByPrefix(CARROT2_PREFIX).getAsMap());
            controller.init(c2SettingsAsMap);
        } catch (Exception e) {
            throw new ElasticSearchException(
                    "Could not start Carrot2 controller.", e);
        }
    }

    @Override
    protected void doStop() throws ElasticSearchException {
        Controller c = controller;
        controller = null;

        if (c != null) {
            c.close();
        }
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        // Noop.
    }
}
