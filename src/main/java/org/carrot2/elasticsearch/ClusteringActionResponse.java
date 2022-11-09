
package org.carrot2.elasticsearch;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

/** An {@link ActionResponse} for {@link ClusteringAction}. */
public class ClusteringActionResponse extends ActionResponse implements ToXContent {
  /** Clustering-related response fields. */
  public static final class Fields {
    static final String SEARCH_RESPONSE = "search_response";
    static final String CLUSTERS = "clusters";
    static final String INFO = "info";

    // from SearchResponse
    static final String _SCROLL_ID = "_scroll_id";
    static final String _SHARDS = "_shards";
    static final String TOTAL = "total";
    static final String SUCCESSFUL = "successful";
    static final String FAILED = "failed";
    static final String FAILURES = "failures";
    static final String STATUS = "status";
    static final String INDEX = "index";
    static final String SHARD = "shard";
    static final String REASON = "reason";
    static final String TOOK = "took";
    static final String TIMED_OUT = "timed_out";

    /** {@link Fields#INFO} keys. */
    public static final class Info {
      public static final String ALGORITHM = "algorithm";
      public static final String SEARCH_MILLIS = "search-millis";
      public static final String CLUSTERING_MILLIS = "clustering-millis";
      public static final String TOTAL_MILLIS = "total-millis";
      public static final String INCLUDE_HITS = "include-hits";
      public static final String MAX_HITS = "max-hits";
      public static final String LANGUAGES = "languages";
    }
  }

  private SearchResponse searchResponse;
  private DocumentGroup[] topGroups;
  private Map<String, String> info;

  ClusteringActionResponse(StreamInput in) throws IOException {
    boolean hasSearchResponse = in.readBoolean();
    if (hasSearchResponse) {
      this.searchResponse = new SearchResponse(in);
    }

    int documentGroupsCount = in.readVInt();
    topGroups = new DocumentGroup[documentGroupsCount];
    for (int i = 0; i < documentGroupsCount; i++) {
      DocumentGroup group = new DocumentGroup(in);
      topGroups[i] = group;
    }

    int entries = in.readVInt();
    info = new LinkedHashMap<>();
    for (int i = 0; i < entries; i++) {
      info.put(in.readOptionalString(), in.readOptionalString());
    }
  }

  public ClusteringActionResponse(
      SearchResponse searchResponse, DocumentGroup[] topGroups, Map<String, String> info) {
    this.searchResponse = Preconditions.checkNotNull(searchResponse);
    this.topGroups = Preconditions.checkNotNull(topGroups);
    this.info = Collections.unmodifiableMap(Preconditions.checkNotNull(info));
  }

  public SearchResponse getSearchResponse() {
    return searchResponse;
  }

  public DocumentGroup[] getDocumentGroups() {
    return topGroups;
  }

  public Map<String, String> getInfo() {
    return info;
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    if (searchResponse != null) {
      searchResponse.innerToXContent(builder, ToXContent.EMPTY_PARAMS);
    }

    builder.startArray(Fields.CLUSTERS);
    if (topGroups != null) {
      for (DocumentGroup group : topGroups) {
        group.toXContent(builder, params);
      }
    }
    builder.endArray();
    builder.field(Fields.INFO, info);
    return builder;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    boolean hasSearchResponse = searchResponse != null;
    out.writeBoolean(hasSearchResponse);
    if (hasSearchResponse) {
      this.searchResponse.writeTo(out);
    }

    out.writeVInt(topGroups == null ? 0 : topGroups.length);
    if (topGroups != null) {
      for (DocumentGroup group : topGroups) {
        group.writeTo(out);
      }
    }

    out.writeVInt(info == null ? 0 : info.size());
    if (info != null) {
      for (Map.Entry<String, String> e : info.entrySet()) {
        out.writeOptionalString(e.getKey());
        out.writeOptionalString(e.getValue());
      }
    }
  }

  @Override
  public String toString() {
    return ToString.objectToJson(this);
  }
}
