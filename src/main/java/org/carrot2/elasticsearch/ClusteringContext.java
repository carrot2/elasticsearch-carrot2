package org.carrot2.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.carrot2.clustering.ClusteringAlgorithmProvider;
import org.carrot2.language.LanguageComponents;
import org.carrot2.language.LanguageComponentsProvider;
import org.carrot2.util.ResourceLookup;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.carrot2.elasticsearch.ClusteringPlugin.DEFAULT_RESOURCES_PROPERTY_NAME;

/**
 * Holds the language components initialized and ready throughout
 * the {@link Node}'s lifecycle.
 */
public class ClusteringContext extends AbstractLifecycleComponent {
   public static final String ATTR_RESOURCE_LOOKUP = "esplugin.resources";

   private final Environment environment;
   private final LinkedHashMap<String, ClusteringAlgorithmProvider> algorithmProviders;
   private final LinkedHashMap<String, List<LanguageComponentsProvider>> languageComponentProviders;

   private Logger logger;
   private ResourceLookup resourceLookup;
   private LinkedHashMap<String, LanguageComponents> languages;

   public ClusteringContext(Environment environment,
                            LinkedHashMap<String, ClusteringAlgorithmProvider> algorithmProviders,
                            LinkedHashMap<String, List<LanguageComponentsProvider>> languageComponentProviders) {
      this.environment = environment;
      this.logger = LogManager.getLogger("plugin.carrot2");
      this.algorithmProviders = algorithmProviders;
      this.languageComponentProviders = languageComponentProviders;
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

         if (c2Settings.get(DEFAULT_RESOURCES_PROPERTY_NAME) != null) {
            final Path resourcesPath = pluginConfigPath.resolve(c2Settings.get(DEFAULT_RESOURCES_PROPERTY_NAME))
                .toAbsolutePath()
                .normalize();
            logger.info("Resources located at: {}", resourcesPath);
            resourceLookup = new PathResourceLookup(resourcesPath);
         } else {
            resourceLookup = null;
            logger.info("Resources read from default locations (JARs).");
         }

         AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            languages = new LinkedHashMap<>();
            for (Map.Entry<String, List<LanguageComponentsProvider>> e : languageComponentProviders.entrySet()) {
               String language = e.getKey();
               languages.put(language, new LanguageComponents(language,
                   componentSuppliers(language, resourceLookup, e.getValue())));
            }

            // Remove languages for which there are no algorithms that support them.
            languages.entrySet().removeIf(e -> {
                   LanguageComponents lc = e.getValue();
                   return algorithmProviders.values()
                       .stream()
                       .noneMatch(algorithm -> algorithm.get().supports(lc));
                });

            logger.info("Available clustering algorithms: {}",
                String.join(", ", algorithmProviders.keySet()));

            algorithmProviders.forEach((name, prov) -> {
               String supportedLanguages = languages.values().stream()
                   .filter(lc -> prov.get().supports(lc))
                   .map(LanguageComponents::language)
                   .collect(Collectors.joining(", "));

               logger.info("Algorithm {} supports the following languages: {}",
                   name, supportedLanguages);
            });

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

   private Map<Class<?>, Supplier<?>> componentSuppliers(String language,
                                                         ResourceLookup resourceLookup,
                                                         List<LanguageComponentsProvider> providers) {
      Map<Class<?>, Supplier<?>> suppliers = new HashMap<>();
      for (LanguageComponentsProvider provider : providers) {
         try {
            Map<Class<?>, Supplier<?>> components =
                resourceLookup == null
                    ? provider.load(language)
                    : provider.load(language, resourceLookup);

            components.forEach((clazz, supplier) -> {
               Supplier<?> existing = suppliers.put(clazz, supplier);
               if (existing != null) {
                  throw new RuntimeException(
                      String.format(
                          Locale.ROOT,
                          "Language '%s' has multiple providers of component '%s': %s",
                          language,
                          clazz.getSimpleName(),
                          Stream.of(existing, supplier)
                              .map(s -> s.getClass().getName())
                              .collect(Collectors.joining(", "))));

               }
            });
         } catch (IOException e) {
            logger.warn(String.format(Locale.ROOT,
                "Could not load resources for language '%s' of provider '%s', provider ignored for this language.",
                language,
                provider.name()));
         }
      }
      return suppliers;
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
