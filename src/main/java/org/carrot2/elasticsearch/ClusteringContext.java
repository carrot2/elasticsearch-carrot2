package org.carrot2.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.carrot2.clustering.ClusteringAlgorithmProvider;
import org.carrot2.language.LanguageComponents;
import org.carrot2.util.ResourceLookup;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ServiceLoader;

import static org.carrot2.elasticsearch.ClusteringPlugin.DEFAULT_RESOURCES_PROPERTY_NAME;

/**
 * Holds the language components initialized and ready throughout
 * the {@link Node}'s lifecycle.
 */
public class ClusteringContext extends AbstractLifecycleComponent {
   public static final String ATTR_RESOURCE_LOOKUP = "esplugin.resources";

   private final Environment environment;
   private LinkedHashMap<String, ClusteringAlgorithmProvider> algorithmProviders;
   private Logger logger;
   private ResourceLookup resourceLookup;
   private LinkedHashMap<String, LanguageComponents> languages;

   public ClusteringContext(Environment environment) {
      this.environment = environment;
      this.logger = LogManager.getLogger("plugin.carrot2");
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

         for (String configName : new String[]{
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
               logger.warn("Could not parse: " + configName, e);
            }
         }
         Settings c2Settings = builder.build();

         algorithmProviders = new LinkedHashMap<>();
         ServiceLoader.load(ClusteringAlgorithmProvider.class,
             ClusteringAlgorithmProvider.class.getClassLoader()).forEach((prov) -> {
            algorithmProviders.put(prov.name(), prov);
         });
         logger.info("Available clustering components: {}",
             String.join(", ", algorithmProviders.keySet()));

         if (c2Settings.get(DEFAULT_RESOURCES_PROPERTY_NAME) != null) {
            final Path resourcesPath = pluginConfigPath.resolve(c2Settings.get(DEFAULT_RESOURCES_PROPERTY_NAME))
                .toAbsolutePath()
                .normalize();
            logger.info("Resources located at: {}", resourcesPath);
            resourceLookup = new PathResourceLookup(resourcesPath);
         } else {
            logger.info("Resources read from default locations (JARs).");
            resourceLookup = null;
         }

         AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            languages = new LinkedHashMap<>();
            for (String language : LanguageComponents.languages()) {
               try {
                  LanguageComponents components =
                      resourceLookup != null
                          ? LanguageComponents.load(language, resourceLookup)
                          : LanguageComponents.load(language);
                  languages.put(language, components);
               } catch (Exception e) {
                  logger.warn("Could not load resources for language: '"
                      + language + "', language will be ignored.", e);
               }
            }
            logger.info("Available languages: {}",
                String.join(", ", languages.keySet()));

            return null;
         });

/*
         // TODO: Set up the license provider for Lingo3G
         Path lingo3gLicense = scanForLingo3GLicense(environment, pluginConfigPath);
         if (lingo3gLicense != null && Files.isReadable(lingo3gLicense)) {
           c2SettingsAsMap.put("license", new FileResource(lingo3gLicense));
         } else if (algorithms.contains("lingo3g")) {
           logger.warn("Lingo3G is on classpath, but no licenses have been found. Check out the documentation.");
         }
*/
      } catch (Exception e) {
         throw new ElasticsearchException(
             "Could not initialize clustering.", e);
      }

      if (algorithmProviders == null || algorithmProviders.isEmpty()) {
         throw new ElasticsearchException("No registered/ available clustering algorithms? Check the logs, it's odd.");
      }
   }

   /**
    * Because we're running with a security manager (most likely), we need to scan for Lingo3G
    * license in ES configuration directories.
    */
   private Path scanForLingo3GLicense(Environment environment, Path pluginConfigPath) {
      List<Path> licenses = new ArrayList<>();

      for (Path candidate : new Path[]{
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

      switch (licenses.size()) {
         case 0:
            return null;
         case 1:
            return licenses.iterator().next();
         default:
            throw new ElasticsearchException("There should be exactly one Lingo3G license on scan paths: {}", licenses);
      }
   }

   /**
    * Return a list of available algorithm component identifiers.
    */
   public LinkedHashMap<String, ClusteringAlgorithmProvider> getAlgorithms() {
      return algorithmProviders;
   }

   @Override
   protected void doStop() throws ElasticsearchException {
      // Noop.
   }

   @Override
   protected void doClose() throws ElasticsearchException {
      // Noop.
   }

   public LanguageComponents getLanguageComponents(String lang) {
      return languages.get(lang);
   }

   public boolean isLanguageSupported(String langCode) {
      return languages.containsKey(langCode);
   }
}
