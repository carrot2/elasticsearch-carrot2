package org.carrot2.elasticsearch;

import org.carrot2.clustering.ClusteringAlgorithmProvider;
import org.carrot2.elasticsearch.ClusteringAction.TransportClusteringAction;
import org.carrot2.language.LanguageComponentsProvider;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.Supplier;

public class ClusteringPlugin extends Plugin implements ExtensiblePlugin, ActionPlugin {
   /**
    * Master on/off switch property for the plugin (general settings).
    */
   public static final String DEFAULT_ENABLED_PROPERTY_NAME = "carrot2.enabled";

   /**
    * Plugin name.
    */
   public static final String PLUGIN_NAME = "elasticsearch-carrot2";

   /**
    * All algorithm providers.
    */
   private LinkedHashMap<String, ClusteringAlgorithmProvider> algorithmProviders = new LinkedHashMap<>();

   /**
    * All language component providers.
    */
   private Map<String, List<LanguageComponentsProvider>> languageComponentProviders = new LinkedHashMap<>();

   private final boolean transportClient;
   private final boolean pluginEnabled;

   public ClusteringPlugin(Settings settings) {
      this.pluginEnabled = settings.getAsBoolean(DEFAULT_ENABLED_PROPERTY_NAME, true);
      this.transportClient = TransportClient.CLIENT_TYPE.equals(Client.CLIENT_TYPE_SETTING_S.get(settings));

      // Invoke loadSPI on our own class loader to load default algorithms.
      reloadSPI(getClass().getClassLoader());
   }

   @Override
   public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
      if (pluginEnabled) {
         return Arrays.asList(
             new ActionHandler<>(ClusteringAction.INSTANCE, TransportClusteringAction.class),
             new ActionHandler<>(ListAlgorithmsAction.INSTANCE, ListAlgorithmsAction.TransportListAlgorithmsAction.class));
      }
      return Collections.emptyList();
   }

   @Override
   public List<RestHandler> getRestHandlers(Settings settings, RestController restController,
                                            ClusterSettings clusterSettings,
                                            IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                            IndexNameExpressionResolver indexNameExpressionResolver,
                                            Supplier<DiscoveryNodes> nodesInCluster) {
      return Arrays.asList(
          new ClusteringAction.RestClusteringAction(restController),
          new ListAlgorithmsAction.RestListAlgorithmsAction(restController));
   }

   @Override
   public Collection<Object> createComponents(Client client, ClusterService clusterService,
                                              ThreadPool threadPool, ResourceWatcherService resourceWatcherService,
                                              ScriptService scriptService, NamedXContentRegistry xContentRegistry,
                                              Environment environment, NodeEnvironment nodeEnvironment,
                                              NamedWriteableRegistry namedWriteableRegistry) {
      List<Object> components = new ArrayList<>();
      if (pluginEnabled && !transportClient) {
         components.add(new ClusteringContext(environment,
             reorderAlgorithms(algorithmProviders),
             new LinkedHashMap<>(languageComponentProviders)));
      }
      return components;
   }

   /**
    * This places Lingo3G in front of the algorithm list if it is available.
    */
   private LinkedHashMap<String, ClusteringAlgorithmProvider> reorderAlgorithms(
       LinkedHashMap<String, ClusteringAlgorithmProvider> providers) {
      String[] desiredOrder = {
          "Lingo3G",
          "Lingo",
          "STC",
          "Bisecting K-Means"
      };
      LinkedHashMap<String, ClusteringAlgorithmProvider> copy = new LinkedHashMap<>();
      for (String name : desiredOrder) {
         if (providers.containsKey(name)) {
            copy.put(name, providers.get(name));
         }
      }
      providers.forEach((name, provider) -> {
         if (!copy.containsKey(name)) {
            copy.put(name, provider);
         }
      });
      return copy;
   }

   @Override
   public void reloadSPI(ClassLoader loader) {
      ServiceLoader.load(ClusteringAlgorithmProvider.class, loader).forEach((provider) -> {
         String name = provider.name();
         if (algorithmProviders.containsKey(name)) {
            throw new RuntimeException("More than one provider for algorithm " + name + "?");
         }
         algorithmProviders.put(name, provider);
      });

      for (LanguageComponentsProvider provider :
          ServiceLoader.load(LanguageComponentsProvider.class, loader)) {
         for (String lang : provider.languages()) {
            languageComponentProviders
                .computeIfAbsent(lang, (k) -> new ArrayList<>())
                .add(provider);
         }
      }
   }
}
