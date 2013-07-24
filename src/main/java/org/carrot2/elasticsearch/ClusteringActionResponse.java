package org.carrot2.elasticsearch;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.base.Preconditions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;

import com.google.common.collect.Maps;

/** */
public class ClusteringActionResponse extends ActionResponse implements ToXContent {
    /**
     * Clustering-related response fields. 
     */
    static final class Fields {
        static final XContentBuilderString SEARCH_RESPONSE = new XContentBuilderString("search_response");
        static final XContentBuilderString CLUSTERS = new XContentBuilderString("clusters");
        static final XContentBuilderString INFO = new XContentBuilderString("info");
        
        /**
         * {@link Fields#INFO} keys.
         */
        static final class Info {
            public static final String ALGORITHM = "algorithm";
            public static final String SEARCH_MILLIS = "search-millis";
            public static final String CLUSTERING_MILLIS = "clustering-millis";
            public static final String TOTAL_MILLIS = "total-millis";
        }
    }

    private SearchResponse searchResponse;
    private DocumentGroup [] topGroups;
    private Map<String,String> info;

    ClusteringActionResponse() {
    }

    public ClusteringActionResponse(
            SearchResponse searchResponse, 
            DocumentGroup[] topGroups,
            Map<String,String> info) {
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params)
            throws IOException {
        if (searchResponse != null) {
            searchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
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
        super.writeTo(out);
        
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
            for (Map.Entry<String,String> e : info.entrySet()) {
                out.writeOptionalString(e.getKey());
                out.writeOptionalString(e.getValue());
            }
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        boolean hasSearchResponse = in.readBoolean();
        if (hasSearchResponse) {
            this.searchResponse = new SearchResponse();
            this.searchResponse.readFrom(in);
        }

        int documentGroupsCount = in.readVInt();
        topGroups = new DocumentGroup [documentGroupsCount];
        for (int i = 0; i < documentGroupsCount; i++) {
            DocumentGroup group = new DocumentGroup();
            group.readFrom(in);
            topGroups[i] = group;
        }
        
        int entries = in.readVInt();
        info = Maps.newLinkedHashMap();
        for (int i = 0; i < entries; i++) {
            info.put(in.readOptionalString(), in.readOptionalString());
        }
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
