package org.carrot2.elasticsearch;

import static org.carrot2.elasticsearch.ClusteringPlugin.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
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
import org.carrot2.util.resource.FileResource;
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
public class ControllerSingleton extends AbstractLifecycleComponent<ControllerSingleton> {
    private final Environment environment;
    private Controller controller;
    private List<String> algorithms;
    private ESLogger logger;

    @Inject
    public ControllerSingleton(Settings settings, Environment environment) {
        super(settings);
        this.environment = environment;
        this.logger = Loggers.getLogger("plugin.carrot2", settings);
    }

    @SuppressForbidden(reason = "C2 integration (File API)")
    @Override
    protected void doStart() throws ElasticsearchException {
        try {
            Settings.Builder builder = Settings.builder();
            Path pluginConfigPath = environment.configFile().resolve(ClusteringPlugin.PLUGIN_NAME);

            if (!Files.isDirectory(pluginConfigPath)) {
              throw new ElasticsearchException("Missing config files: {}", pluginConfigPath);
            } else {
              logger.info("Configuration files at: {}", pluginConfigPath.toAbsolutePath());
            }

            for (String configName : new String [] {
                    "config.yml",
                    "config.yaml",
                    "config.json",
                    "config.properties"
            }) {
                try {
                    Path resolved = pluginConfigPath.resolve(configName);
                    if (resolved != null && Files.exists(resolved)) {
                        builder.loadFromPath(resolved);
                    }
                } catch (NoClassDefFoundError e) {
                    logger.warn("Could not parse: {}", e, configName);
                }
            }
            Settings c2Settings = builder.build();

            // Parse suite descriptors with loggers turned off (shut them up a bit).
            final Path suitePath = pluginConfigPath.resolve(c2Settings.get(DEFAULT_SUITE_PROPERTY_NAME));
            if (!Files.isRegularFile(suitePath)) {
                throw new ElasticsearchException(
                        "Could not find algorithm suite: " + suitePath.toAbsolutePath().normalize());
            }

            final ResourceLookup suiteLookup = new ResourceLookup(
                new DirLocator(suitePath.getParent().toFile()));
            final IResource suiteResource = 
                suiteLookup.getFirst(suitePath.getFileName().toString());

            final List<String> failed = Lists.newArrayList();
            final ProcessingComponentSuite suite = AccessController.doPrivileged(new   PrivilegedExceptionAction<ProcessingComponentSuite>() {
              @Override
              public ProcessingComponentSuite run() throws Exception {
                return LoggerUtils.quietCall(new Callable<ProcessingComponentSuite>() {
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
              }
            });

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

            final Path resourcesPath = pluginConfigPath.resolve(c2Settings.get(DEFAULT_RESOURCES_PROPERTY_NAME, "."))
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

            // Set up the license for Lingo3G, if it's available.
            Path lingo3gLicense = scanForLingo3GLicense(environment, pluginConfigPath);
            if (lingo3gLicense != null && Files.isReadable(lingo3gLicense)) {
              c2SettingsAsMap.put("license", new FileResource(lingo3gLicense.toFile()));
            } else if (algorithms.contains("lingo3g")) {
              logger.warn("Lingo3G is on classpath, but no licenses have been found. Check out the documentation.");
            }

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

    /**
     * Because we're running with a security manager (most likely), we need to scan for Lingo3G
     * license in ES configuration directories.
     */
    private Path scanForLingo3GLicense(Environment environment, Path pluginConfigPath) {
      List<Path> licenses = new ArrayList<>();

      for (Path candidate : new Path [] {
          pluginConfigPath.resolve("license.xml"),
          pluginConfigPath.resolve(".license.xml"),
          environment.configFile().resolve("license.xml"),
          environment.configFile().resolve(".license.xml")
      }) {
        logger.debug("Lingo3G license location scan: {} {}.",
            candidate.toAbsolutePath().normalize(),
            Files.isRegularFile(candidate) ? "(found)" : "(not found)");
        if (Files.isRegularFile(candidate)) {
          licenses.add(candidate);
        }
      }

      if (licenses.size() > 1) {
        throw new ElasticsearchException("There should be exactly one Lingo3G license on scan paths: {}", licenses);
      }

      if (licenses.size() == 1) {
        return licenses.iterator().next();
      } else {
        return null;
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
        final Controller c = controller;
        controller = null;

        if (c != null) {
          AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
              c.close();
              return null;
            }
          });
        }
    }

    @Override
    protected void doClose() throws ElasticsearchException {
        // Noop.
    }
}
