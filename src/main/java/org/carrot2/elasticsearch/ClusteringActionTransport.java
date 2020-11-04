package org.carrot2.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.carrot2.attrs.Attrs;
import org.carrot2.clustering.Cluster;
import org.carrot2.clustering.ClusteringAlgorithm;
import org.carrot2.clustering.ClusteringAlgorithmProvider;
import org.carrot2.clustering.Document;
import org.carrot2.language.LanguageComponents;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

/** A {@link TransportAction} for {@link ClusteringAction}. */
public class ClusteringActionTransport
    extends TransportAction<ClusteringActionRequest, ClusteringActionResponse> {
  protected Logger logger = LogManager.getLogger(getClass());

  private final TransportSearchAction searchAction;
  private final ClusteringContext context;

  @Inject
  public ClusteringActionTransport(
      TransportService transportService,
      TransportSearchAction searchAction,
      ClusteringContext controllerSingleton,
      ActionFilters actionFilters) {
    super(ClusteringAction.NAME, actionFilters, transportService.getTaskManager());

    this.searchAction = searchAction;
    this.context = controllerSingleton;
    transportService.registerRequestHandler(
        ClusteringAction.NAME,
        ThreadPool.Names.SAME,
        ClusteringActionRequest::new,
        new TransportHandler());
  }

  @Override
  protected void doExecute(
      Task task,
      final ClusteringActionRequest clusteringRequest,
      final ActionListener<ClusteringActionResponse> listener) {
    final long tsSearchStart = System.nanoTime();
    searchAction.execute(
        clusteringRequest.getSearchRequest(),
        new ActionListener<>() {
          @Override
          public void onFailure(Exception e) {
            listener.onFailure(e);
          }

          @Override
          public void onResponse(SearchResponse response) {
            final long tsSearchEnd = System.nanoTime();

            LinkedHashMap<String, ClusteringAlgorithmProvider> algorithms = context.getAlgorithms();

            final String algorithmId =
                requireNonNullElse(
                    clusteringRequest.getAlgorithm(), algorithms.keySet().iterator().next());

            ClusteringAlgorithmProvider provider = algorithms.get(algorithmId);
            if (provider == null) {
              listener.onFailure(new IllegalArgumentException("No such algorithm: " + algorithmId));
              return;
            }

            /*
             * We're not a threaded listener so we're running on the search thread. This
             * is good -- we don't want to serve more clustering requests than we can handle
             * anyway.
             */
            ClusteringAlgorithm algorithm = provider.get();

            try {
              Map<String, Object> requestAttrs = clusteringRequest.getAttributes();
              if (requestAttrs != null) {
                Attrs.populate(algorithm, requestAttrs);
              }

              String queryHint = clusteringRequest.getQueryHint();
              if (queryHint != null) {
                algorithm.accept(
                    new OptionalQueryHintSetterVisitor(clusteringRequest.getQueryHint()));
              }

              List<InputDocument> documents =
                  prepareDocumentsForClustering(clusteringRequest, response);

              String defaultLanguage = clusteringRequest.getDefaultLanguage();
              if (!context.isLanguageSupported(defaultLanguage)) {
                throw new RuntimeException(
                    "The requested default language is not supported: '" + defaultLanguage + "'");
              }

              // Split documents into language groups.
              Map<String, List<InputDocument>> documentsByLanguage =
                  documents.stream()
                      .collect(
                          Collectors.groupingBy(
                              doc -> {
                                String lang = doc.language();
                                return lang == null ? defaultLanguage : lang;
                              }));

              // Run clustering.
              long tsClusteringTotal = 0;
              HashSet<String> warnOnce = new HashSet<>();
              LinkedHashMap<String, List<Cluster<InputDocument>>> clustersByLanguage =
                  new LinkedHashMap<>();
              for (Map.Entry<String, List<InputDocument>> e : documentsByLanguage.entrySet()) {
                String lang = e.getKey();
                if (!context.isLanguageSupported(lang)) {
                  if (warnOnce.add(lang)) {
                    logger.warn(
                        "Language is not supported, documents in this "
                            + "language will not be clustered: '"
                            + lang
                            + "'");
                  }
                } else {
                  LanguageComponents languageComponents = context.getLanguageComponents(lang);
                  final long tsClusteringStart = System.nanoTime();
                  clustersByLanguage.put(
                      lang, algorithm.cluster(e.getValue().stream(), languageComponents));
                  final long tsClusteringEnd = System.nanoTime();
                  tsClusteringTotal += (tsClusteringEnd - tsClusteringStart);
                }
              }

              final Map<String, String> info = new LinkedHashMap<>();
              info.put(ClusteringActionResponse.Fields.Info.ALGORITHM, algorithmId);
              info.put(
                  ClusteringActionResponse.Fields.Info.SEARCH_MILLIS,
                  Long.toString(TimeUnit.NANOSECONDS.toMillis(tsSearchEnd - tsSearchStart)));
              info.put(
                  ClusteringActionResponse.Fields.Info.CLUSTERING_MILLIS,
                  Long.toString(TimeUnit.NANOSECONDS.toMillis(tsClusteringTotal)));
              info.put(
                  ClusteringActionResponse.Fields.Info.TOTAL_MILLIS,
                  Long.toString(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - tsSearchStart)));
              info.put(
                  ClusteringActionResponse.Fields.Info.MAX_HITS,
                  clusteringRequest.getMaxHits() == Integer.MAX_VALUE
                      ? ""
                      : Integer.toString(clusteringRequest.getMaxHits()));
              info.put(
                  ClusteringActionResponse.Fields.Info.LANGUAGES,
                  String.join(", ", clustersByLanguage.keySet()));

              // Trim search response's hits if we need to.
              if (clusteringRequest.getMaxHits() != Integer.MAX_VALUE) {
                response = filterMaxHits(response, clusteringRequest.getMaxHits());
              }

              AtomicInteger groupId = new AtomicInteger();
              Map<String, DocumentGroup[]> adaptedByLanguage =
                  clustersByLanguage.entrySet().stream()
                      .filter(e -> !e.getValue().isEmpty())
                      .collect(
                          Collectors.toMap(Map.Entry::getKey, e -> adapt(e.getValue(), groupId)));

              final ArrayList<DocumentGroup> groups = new ArrayList<>();
              adaptedByLanguage
                  .values()
                  .forEach(langClusters -> groups.addAll(Arrays.asList(langClusters)));

              if (adaptedByLanguage.size() > 1) {
                groups.sort(
                    (a, b) ->
                        Integer.compare(b.uniqueDocuments().size(), a.uniqueDocuments().size()));
              }

              if (clusteringRequest.createUngroupedDocumentsCluster) {
                DocumentGroup ungrouped = new DocumentGroup();
                ungrouped.setId(groupId.incrementAndGet());
                ungrouped.setPhrases(new String[] {"Ungrouped documents"});
                ungrouped.setUngroupedDocuments(true);
                ungrouped.setScore(0d);

                LinkedHashSet<InputDocument> ungroupedDocuments = new LinkedHashSet<>(documents);
                clustersByLanguage
                    .values()
                    .forEach(langClusters -> removeReferenced(ungroupedDocuments, langClusters));
                ungrouped.setDocumentReferences(
                    ungroupedDocuments.stream()
                        .map(InputDocument::getStringId)
                        .toArray(String[]::new));

                groups.add(ungrouped);
              }

              listener.onResponse(
                  new ClusteringActionResponse(
                      response, groups.toArray(new DocumentGroup[0]), info));
            } catch (Exception e) {
              // Log a full stack trace with all nested exceptions but only return
              // ElasticSearchException exception with a simple String (otherwise
              // clients cannot deserialize exception classes).
              String message = "Clustering error: " + e.getMessage();
              logger.warn(message, e);
              listener.onFailure(new ElasticsearchException(message));
            }
          }

          private void removeReferenced(
              LinkedHashSet<InputDocument> ungrouped, List<Cluster<InputDocument>> clusters) {
            clusters.forEach(
                cluster -> {
                  ungrouped.removeAll(cluster.getDocuments());
                  removeReferenced(ungrouped, cluster.getClusters());
                });
          }
        });
  }

  public static <T> T requireNonNullElse(T first, T def) {
    return first != null ? first : def;
  }

  protected SearchResponse filterMaxHits(SearchResponse response, int maxHits) {
    // We will use internal APIs here for efficiency. The plugin has restricted explicit ES
    // compatibility
    // anyway. Alternatively, we could serialize/ filter/ deserialize JSON, but this seems
    // simpler.
    SearchHits allHits = response.getHits();
    SearchHit[] trimmedHits = new SearchHit[Math.min(maxHits, allHits.getHits().length)];
    System.arraycopy(allHits.getHits(), 0, trimmedHits, 0, trimmedHits.length);

    InternalAggregations _internalAggregations = null;
    if (response.getAggregations() != null) {
      _internalAggregations =
          new InternalAggregations(toInternal(response.getAggregations().asList()), null);
    }

    SearchHits _searchHits =
        new SearchHits(trimmedHits, allHits.getTotalHits(), allHits.getMaxScore());

    SearchProfileShardResults _searchProfileShardResults =
        new SearchProfileShardResults(response.getProfileResults());

    InternalSearchResponse _searchResponse =
        new InternalSearchResponse(
            _searchHits,
            _internalAggregations,
            response.getSuggest(),
            _searchProfileShardResults,
            response.isTimedOut(),
            response.isTerminatedEarly(),
            response.getNumReducePhases());

    return new SearchResponse(
        _searchResponse,
        response.getScrollId(),
        response.getTotalShards(),
        response.getSuccessfulShards(),
        response.getSkippedShards(),
        response.getTook().getMillis(),
        response.getShardFailures(),
        response.getClusters());
  }

  private List<InternalAggregation> toInternal(List<Aggregation> list) {
    List<InternalAggregation> t = new ArrayList<>(list.size());
    for (Aggregation a : list) {
      t.add((InternalAggregation) a);
    }
    return t;
  }

  protected DocumentGroup[] adapt(List<Cluster<InputDocument>> clusters, AtomicInteger groupId) {
    DocumentGroup[] groups = new DocumentGroup[clusters.size()];
    for (int i = 0; i < groups.length; i++) {
      groups[i] = adapt(clusters.get(i), groupId);
    }
    return groups;
  }

  private DocumentGroup adapt(Cluster<InputDocument> cluster, AtomicInteger groupId) {
    DocumentGroup group = new DocumentGroup();
    group.setId(groupId.incrementAndGet());
    group.setPhrases(cluster.getLabels().toArray(new String[0]));
    group.setScore(cluster.getScore());

    List<InputDocument> documents = cluster.getDocuments();
    String[] documentReferences = new String[documents.size()];
    for (int i = 0; i < documentReferences.length; i++) {
      documentReferences[i] = documents.get(i).getStringId();
    }
    group.setDocumentReferences(documentReferences);
    group.setSubgroups(adapt(cluster.getClusters(), groupId));

    return group;
  }

  /** Map {@link SearchHit} fields to logical fields of Carrot2 {@link Document}. */
  private List<InputDocument> prepareDocumentsForClustering(
      final ClusteringActionRequest request, SearchResponse response) {
    SearchHit[] hits = response.getHits().getHits();
    List<InputDocument> documents = new ArrayList<>(hits.length);
    List<FieldMappingSpec> fieldMapping = request.getFieldMapping();
    StringBuilder title = new StringBuilder();
    StringBuilder content = new StringBuilder();
    StringBuilder language = new StringBuilder();
    boolean emptySourceWarningEmitted = false;

    for (SearchHit hit : hits) {
      // Prepare logical fields for each hit.
      title.setLength(0);
      content.setLength(0);
      language.setLength(0);

      Map<String, DocumentField> fields = hit.getFields();
      Map<String, HighlightField> highlightFields = hit.getHighlightFields();

      Map<String, Object> sourceAsMap = null;
      for (FieldMappingSpec spec : fieldMapping) {
        // Determine the content source.
        Object appendContent = null;
        outer:
        switch (spec.source) {
          case FIELD:
            DocumentField hitField = fields.get(spec.field);
            if (hitField != null) {
              appendContent = hitField.getValue();
            }
            break;

          case HIGHLIGHT:
            HighlightField highlightField = highlightFields.get(spec.field);
            if (highlightField != null) {
              appendContent = join(Arrays.asList(highlightField.fragments()));
            }
            break;

          case SOURCE:
            if (sourceAsMap == null) {
              if (!hit.hasSource()) {
                if (!emptySourceWarningEmitted) {
                  emptySourceWarningEmitted = true;
                  logger.warn(
                      "_source field mapping used but no source available for: {}, field {}",
                      hit.getId(),
                      spec.field);
                }
              } else {
                sourceAsMap = hit.getSourceAsMap();
              }
            }

            if (sourceAsMap != null) {
              String[] fieldNames = spec.field.split("\\.");
              Object value = sourceAsMap;

              // Descend into maps.
              for (String fieldName : fieldNames) {
                if (Map.class.isInstance(value)) {
                  value = ((Map<?, ?>) value).get(fieldName);
                  if (value == null) {
                    // No such key.
                    logger.warn(
                        "Cannot find field named '{}' from spec: '{}'", fieldName, spec.field);
                    break outer;
                  }
                } else {
                  logger.warn("Field is not a map: {} in spec.: {}", fieldName, spec.field);
                  break outer;
                }
              }

              if (value instanceof List) {
                appendContent = join((List<?>) value);
              } else {
                appendContent = value;
              }
            }
            break;

          default:
            throw Preconditions.unreachable();
        }

        // Determine the target field.
        if (appendContent != null) {
          StringBuilder target;
          switch (spec.logicalField) {
            case LANGUAGE:
              language.setLength(0); // Clear previous (single mapping allowed);
              target = language;
              break;
            case TITLE:
              target = title;
              break;
            case CONTENT:
              target = content;
              break;
            default:
              throw Preconditions.unreachable();
          }

          // Separate multiple fields with a single dot (prevent accidental phrase gluing).
          if (target.length() > 0) {
            target.append(" . ");
          }
          target.append(appendContent);
        }
      }

      String langCode = language.length() > 0 ? language.toString() : null;
      InputDocument doc =
          new InputDocument(title.toString(), content.toString(), langCode, hit.getId());

      documents.add(doc);
    }

    return documents;
  }

  static String join(List<?> list) {
    StringBuilder sb = new StringBuilder();
    for (Object t : list) {
      if (sb.length() > 0) {
        sb.append(" . ");
      }
      sb.append(t != null ? t.toString() : "");
    }
    return sb.toString();
  }

  private final class TransportHandler implements TransportRequestHandler<ClusteringActionRequest> {
    @Override
    public void messageReceived(
        final ClusteringActionRequest request, final TransportChannel channel, Task task) {
      execute(
          request,
          new ActionListener<>() {
            @Override
            public void onResponse(ClusteringActionResponse response) {
              try {
                channel.sendResponse(response);
              } catch (Exception e) {
                onFailure(e);
              }
            }

            @Override
            public void onFailure(Exception e) {
              try {
                channel.sendResponse(e);
              } catch (Exception e1) {
                logger.warn(
                    "Failed to send error response for action ["
                        + ClusteringAction.NAME
                        + "] and request ["
                        + request
                        + "]",
                    e1);
              }
            }
          });
    }
  }
}
