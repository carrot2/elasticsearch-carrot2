package org.carrot2.elasticsearch;

import static org.carrot2.elasticsearch.ClusteringPlugin.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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

    @SuppressForbidden(reason = "C2 integration (File API)")
    @Override
    protected void doStart() throws ElasticsearchException {
        try {
            Settings.Builder builder = Settings.builder();
            Path configPath = environment.configFile().resolve(ClusteringPlugin.PLUGIN_NAME);

            if (!Files.isDirectory(configPath)) {
              Path srcConfig = Paths.get("src/main/config");
              if (Files.isDirectory(srcConfig)) {
                // Allow running from within the IDE.
                configPath = srcConfig;
              } else {
                throw new ElasticsearchException("Config folder missing: " + configPath);
              }
            } else {
              logger.info("Configuration files at: {}", configPath.toAbsolutePath());
            }

            for (String configName : new String [] {
                    "config.yml",
                    "config.yaml",
                    "config.json",
                    "config.properties"
            }) {
                try {
                    Path resolved = configPath.resolve(configName);
                    if (resolved != null && Files.exists(resolved)) {
                        builder.loadFromPath(resolved);
                    }
                } catch (NoClassDefFoundError e) {
                    logger.warn("Could not parse: {}", e, configName);
                }
            }
            Settings c2Settings = builder.build();

            // Parse suite descriptors with loggers turned off (shut them up a bit).
            final Path suitePath = configPath.resolve(c2Settings.get(DEFAULT_SUITE_PROPERTY_NAME));
            if (!Files.isRegularFile(suitePath)) {
                throw new ElasticsearchException(
                        "Could not find algorithm suite: " + suitePath.toAbsolutePath().normalize());
            }

            final ResourceLookup suiteLookup = new ResourceLookup(
                new DirLocator(suitePath.getParent().toFile()));
            final IResource suiteResource = 
                suiteLookup.getFirst(suitePath.getFileName().toString());

            final List<String> failed = Lists.newArrayList();
            final ProcessingComponentSuite suite = LoggerUtils.quietCall(new Callable<ProcessingComponentSuite>() {
                public ProcessingComponentSuite call() throws Exception {
                    ProcessingComponentSuite suite = ProcessingComponentSuite.deserialize(
                            suiteResource, suiteLookup);
                    for (ProcessingComponentDescriptor desc : suite.removeUnavailableComponents()) {
                        failed.add(desc.getId());
                        if (isNoClassDefFound(desc.getInitializationFailure())) {
                            logger.debug("Algorithm not available on classpath: {}", desc.getId());
                        } else {
                            logger.warn("Algorithm initialization failed: {}", desc.getInitializationFailure(), desc.getId());
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

            final Path resourcesPath = configPath.resolve(c2Settings.get(DEFAULT_RESOURCES_PROPERTY_NAME, "."))
                .toAbsolutePath()
                .normalize();

            logger.info("Lexical resources dir: {}", resourcesPath);

            final ResourceLookup resourceLookup = new ResourceLookup(
                    new DirLocator(resourcesPath.toFile()),
                    new ClassLoaderLocator(ControllerSingleton.class.getClassLoader()));

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
