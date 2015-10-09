package org.carrot2.elasticsearch;

import static org.carrot2.elasticsearch.ClusteringPlugin.DEFAULT_COMPONENT_SIZE_PROPERTY_NAME;
import static org.carrot2.elasticsearch.ClusteringPlugin.DEFAULT_RESOURCES_PROPERTY_NAME;
import static org.carrot2.elasticsearch.ClusteringPlugin.DEFAULT_SUITE_PROPERTY_NAME;
import static org.carrot2.elasticsearch.ClusteringPlugin.DEFAULT_SUITE_RESOURCE;
import static org.carrot2.elasticsearch.ClusteringPlugin.PLUGIN_CONFIG_FILE_NAME;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.carrot2.core.Controller;
import org.carrot2.core.ControllerFactory;
import org.carrot2.core.ProcessingComponentDescriptor;
import org.carrot2.core.ProcessingComponentSuite;
import org.carrot2.text.linguistic.DefaultLexicalDataFactoryDescriptor;
import org.carrot2.util.ReflectionUtils;
import org.carrot2.util.resource.ClassLoaderLocator;
import org.carrot2.util.resource.DirLocator;
import org.carrot2.util.resource.IResource;
import org.carrot2.util.resource.ResourceLookup;
import org.elasticsearch.ElasticsearchException;

import com.google.common.collect.Maps;

import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Holds the {@link Controller} singleton initialized and ready throughout
 * the {@link Node}'s lifecycle.
 */
class ControllerSingleton extends AbstractLifecycleComponent<ControllerSingleton> {
    private final Environment environment;
    private Controller controller;
    private List<String> algorithms;
    private ESLogger logger;

    @Inject
    protected ControllerSingleton(Settings settings, Environment environment) {
        super(settings);
        this.environment = environment;
        this.logger = Loggers.getLogger("plugin.carrot2", settings);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        try {
            Settings.Builder builder = Settings.builder();
            for (String configName : new String [] {
                    PLUGIN_CONFIG_FILE_NAME + ".yml",
                    PLUGIN_CONFIG_FILE_NAME + ".json",
                    PLUGIN_CONFIG_FILE_NAME + ".properties"
            }) {
                try {
                    builder.loadFromPath(environment.resolveRepoFile(configName));
                } catch (NoClassDefFoundError e) {
                    logger.warn("Could not parse: {}", e, configName);
                }
            }
            Settings c2Settings = builder.build();

            final String resourcesPath = c2Settings.get(DEFAULT_RESOURCES_PROPERTY_NAME, ".");
            final Path resourcesDir = environment.configFile().resolveSibling(resourcesPath)
                        .toAbsolutePath()
                        .normalize();

            logger.info("Resources dir: {}", resourcesDir);

            final ResourceLookup resourceLookup = new ResourceLookup(
                    new DirLocator(resourcesDir.toFile()),
                    new ClassLoaderLocator(ControllerSingleton.class.getClassLoader()));

            // Parse suite's descriptors with loggers turned off (shut them up a bit).
            final String suiteResourceName = c2Settings.get(
                    DEFAULT_SUITE_PROPERTY_NAME, 
                    DEFAULT_SUITE_RESOURCE);
            final IResource suiteResource = resourceLookup.getFirst(suiteResourceName);
            if (suiteResource == null) {
                throw new ElasticsearchException(
                        "Could not find algorithm suite: " + suiteResourceName);
            }

            final List<String> failed = Lists.newArrayList();
            final ProcessingComponentSuite suite = LoggerUtils.quietCall(new Callable<ProcessingComponentSuite>() {
                public ProcessingComponentSuite call() throws Exception {
                    ProcessingComponentSuite suite = ProcessingComponentSuite.deserialize(
                            suiteResource, resourceLookup);
                    for (ProcessingComponentDescriptor desc : suite.removeUnavailableComponents()) {
                        failed.add(desc.getId());
                        if (isNoClassDefFound(desc.getInitializationFailure())) {
                            logger.debug("Algorithm not available on classpath: {}", desc.getId());
                        } else {
                            logger.debug("Algorithm initialization failed: {}", desc.getInitializationFailure(), desc.getId());
                        }
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

            if (!algorithms.isEmpty()) {
                logger.info("Available clustering components: {}", Joiner.on(", ").join(algorithms));
            }
            if (!failed.isEmpty()) {
                logger.info("Unavailable clustering components: {}", Joiner.on(", ").join(failed));
            }

            // Change the default resource lookup to include the configured location.
            Map<String, Object> c2SettingsAsMap = Maps.newHashMap();
            DefaultLexicalDataFactoryDescriptor.attributeBuilder(c2SettingsAsMap)
                .resourceLookup(resourceLookup);
            c2SettingsAsMap.putAll(c2Settings.getAsMap());

            // Create component pool.
            Integer poolSize = c2Settings.getAsInt(DEFAULT_COMPONENT_SIZE_PROPERTY_NAME, 0);
            if (poolSize > 0) {
                controller = ControllerFactory.createPooling(poolSize);
            } else {
                controller = ControllerFactory.createPooling();
            }
            controller.init(c2SettingsAsMap, suite.getComponentConfigurations());
        } catch (Exception e) {
            throw new ElasticsearchException(
                    "Could not start Carrot2 controller.", e);
        }

        if (algorithms == null || algorithms.isEmpty()) {
            throw new ElasticsearchException("No registered/ available clustering algorithms? Check the logs, it's odd.");
        }
    }

    /** */
    protected boolean isNoClassDefFound(Throwable initializationFailure) {
        if (initializationFailure != null) {
            return initializationFailure.getCause() instanceof ClassNotFoundException;
        }
        return false;
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
    protected void doStop() throws ElasticsearchException {
        Controller c = controller;
        controller = null;

        if (c != null) {
            c.close();
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        // Noop.
    }
}
