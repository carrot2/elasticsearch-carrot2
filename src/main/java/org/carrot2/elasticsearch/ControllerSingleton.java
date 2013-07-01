package org.carrot2.elasticsearch;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.carrot2.core.Controller;
import org.carrot2.core.ControllerFactory;
import org.carrot2.core.ProcessingComponentDescriptor;
import org.carrot2.core.ProcessingComponentSuite;
import org.carrot2.util.ReflectionUtils;
import org.carrot2.util.resource.ClassLoaderLocator;
import org.carrot2.util.resource.DirLocator;
import org.carrot2.util.resource.ResourceLookup;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.FailedToResolveConfigException;
import org.elasticsearch.node.Node;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import static org.carrot2.elasticsearch.ClusteringPlugin.*;

/**
 * Holds the {@link Controller} singleton initialized and ready throughout
 * the {@link Node}'s lifecycle.
 */
class ControllerSingleton extends AbstractLifecycleComponent<ControllerSingleton> {
    private final Environment environment;
    private Controller controller;
    private List<String> algorithms;

    @Inject
    protected ControllerSingleton(Settings settings, Environment environment) {
        super(settings);
        this.environment = environment;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
        try {
            Builder builder = ImmutableSettings.builder();
            for (String configName : new String [] {
                    DEFAULT_CONFIG_FILE + ".yml",
                    DEFAULT_CONFIG_FILE + ".json",
                    DEFAULT_CONFIG_FILE + ".properties"
            }) {
                try {
                    builder.loadFromUrl(environment.resolveConfig(configName));
                } catch (FailedToResolveConfigException e) {
                    // fall-through
                } catch (NoClassDefFoundError e) {
                    logger.warn("Could not parse: {}", e, configName);
                }
            }
            Settings c2Settings = builder.build();

            final ResourceLookup resourceLookup = new ResourceLookup(
                    new DirLocator(environment.configFile()),
                    new ClassLoaderLocator(ControllerSingleton.class.getClassLoader()));

            // Parse suite's descriptors with loggers turned off (shut them up a bit).
            final String suiteResource = c2Settings.get(DEFAULT_SUITE_PROPERTY_NAME, "carrot2.suite.xml");
            final List<String> failed = Lists.newArrayList();
            final ProcessingComponentSuite suite = LoggerUtils.quietCall(new Callable<ProcessingComponentSuite>() {
                public ProcessingComponentSuite call() throws Exception {
                    ProcessingComponentSuite suite = ProcessingComponentSuite.deserialize(
                            resourceLookup.getFirst(suiteResource), resourceLookup);
                    for (ProcessingComponentDescriptor desc : suite.removeUnavailableComponents()) {
                        failed.add(desc.getId());
                        logger.debug("Algorithm initialization failed: {}", desc.getInitializationFailure(), desc.getId());
                    }
                    return suite;
                }
            },
            Logger.getLogger(ProcessingComponentDescriptor.class),
            Logger.getLogger(ReflectionUtils.class));
            
            algorithms = Lists.newArrayList();
            for (ProcessingComponentDescriptor descriptor : suite.getAlgorithms()) {
                algorithms.add(descriptor.getId());
            }
            algorithms = Collections.unmodifiableList(algorithms);

            logger.info("Available clustering components: {}", Joiner.on(", ").join(algorithms));
            logger.info("Unavailable clustering components: {}", Joiner.on(", ").join(failed));

            Map<String, Object> c2SettingsAsMap = Maps.newHashMap();
            c2SettingsAsMap.putAll(c2Settings.getAsMap());

            controller = ControllerFactory.createPooling();
            controller.init(c2SettingsAsMap, suite.getComponentConfigurations());
        } catch (Exception e) {
            throw new ElasticSearchException(
                    "Could not start Carrot2 controller.", e);
        }

        if (algorithms == null || algorithms.isEmpty()) {
            throw new ElasticSearchException("No registered/ available clustering algorithms? Check the logs, it's odd.");
        }
    }

    public Controller getController() {
        return controller;
    }
    
    /**
     * Return a list of available algorithm component identifiers.
     */
    public List<String> getAlgorithms() {
        return algorithms;
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
