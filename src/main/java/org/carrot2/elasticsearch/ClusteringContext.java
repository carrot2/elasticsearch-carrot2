
package org.carrot2.elasticsearch;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.carrot2.clustering.ClusteringAlgorithm;
import org.carrot2.clustering.ClusteringAlgorithmProvider;
import org.carrot2.language.LanguageComponents;
import org.carrot2.language.LanguageComponentsLoader;
import org.carrot2.language.LanguageComponentsProvider;
import org.carrot2.language.LoadedLanguages;
import org.carrot2.util.ChainedResourceLookup;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.Node;

/** Holds the language components initialized and ready throughout the {@link Node}'s lifecycle. */
public class ClusteringContext extends AbstractLifecycleComponent {
  public static final String PROP_RESOURCES = "resources";

  private final Environment environment;
  private final LinkedHashMap<String, ClusteringAlgorithmProvider> algorithmProviders;
  private final LinkedHashMap<String, List<LanguageComponentsProvider>> languageComponentProviders;

  private LinkedHashMap<String, LanguageComponents> languages;
  private Logger logger;

  public ClusteringContext(
      Environment environment,
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
      Path esConfig = environment.configFile();
      Path pluginConfigPath = esConfig.resolve(ClusteringPlugin.PLUGIN_NAME);

      if (!Files.isDirectory(pluginConfigPath)) {
        throw new ElasticsearchException("Missing configuration folder?: {}", pluginConfigPath);
      }

      Settings.Builder builder = Settings.builder();
      for (String configName :
          new String[] {"config.yml", "config.yaml", "config.json", "config.properties"}) {
        Path resolved = pluginConfigPath.resolve(configName);
        if (Files.exists(resolved)) {
          builder.loadFromPath(resolved);
        }
      }
      Settings c2Settings = builder.build();

      List<Path> resourceLocations =
          c2Settings.getAsList(PROP_RESOURCES).stream()
              .map(p -> esConfig.resolve(p).toAbsolutePath())
              .filter(
                  p -> {
                    boolean exists = Files.exists(p);
                    if (!exists) {
                      logger.info(
                          "Clustering algorithm resource location does not exist, ignored: {}", p);
                    }
                    return exists;
                  })
              .collect(Collectors.toList());

      LanguageComponentsLoader loader = LanguageComponents.loader();

      if (!resourceLocations.isEmpty()) {
        logger.info(
            "Clustering algorithm resources first looked up relative to: {}", resourceLocations);
        loader.withResourceLookup(
            (provider) ->
                new ChainedResourceLookup(
                    Arrays.asList(
                        new PathResourceLookup(resourceLocations),
                        provider.defaultResourceLookup())));
      } else {
        logger.info("Resources read from defaults (JARs).");
      }

      // Only load the resources of algorithms we're interested in.
      loader.limitToAlgorithms(
          algorithmProviders.values().stream()
              .map(Supplier::get)
              .toArray(ClusteringAlgorithm[]::new));

      AccessController.doPrivileged(
          (PrivilegedExceptionAction<Void>)
              () -> {
                languages = new LinkedHashMap<>();
                LoadedLanguages loadedLanguages = loader.load(languageComponentProviders);
                for (String lang : loadedLanguages.languages()) {
                  languages.put(lang, loadedLanguages.language(lang));
                }

                // Debug info about loaded languages.
                if (logger.isDebugEnabled()) {
                  for (String lang : loadedLanguages.languages()) {
                    logger.trace(
                        "Loaded language '"
                            + lang
                            + "' with components: "
                            + "\n  - "
                            + loadedLanguages.language(lang).components().stream()
                                .map(c -> c.getSimpleName())
                                .collect(Collectors.joining("\n  - ")));
                  }
                }

                // Remove algorithms for which there are no languages that are supported.
                algorithmProviders
                    .entrySet()
                    .removeIf(e -> !isAlgorithmAvailable(e.getValue(), languages.values()));

                algorithmProviders.forEach(
                    (name, prov) -> {
                      String supportedLanguages =
                          languages.values().stream()
                              .filter(lc -> prov.get().supports(lc))
                              .map(LanguageComponents::language)
                              .collect(Collectors.joining(", "));

                      logger.info(
                          "Clustering algorithm {} loaded with support for the following languages: {}",
                          name,
                          supportedLanguages);
                    });

                return null;
              });
    } catch (Exception e) {
      throw new ElasticsearchException("Could not initialize clustering.", e);
    }

    if (algorithmProviders.isEmpty()) {
      throw new ElasticsearchException(
          "No registered/ available clustering algorithms? Check the logs, it's odd.");
    }
  }

  /** @return Return a list of available algorithm component identifiers. */
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

  private boolean isAlgorithmAvailable(
      ClusteringAlgorithmProvider provider, Collection<LanguageComponents> languages) {
    ClusteringAlgorithm algorithm = provider.get();
    Optional<LanguageComponents> first = languages.stream().filter(algorithm::supports).findFirst();
    if (first.isEmpty()) {
      logger.warn("Algorithm does not support any of the available languages: {}", provider.name());
      return false;
    } else {
      return true;
    }
  }
}
