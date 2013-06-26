package org.carrot2.elasticsearch.plugin;

import java.io.IOException;
import java.util.List;

import org.carrot2.core.Cluster;
import org.carrot2.core.Document;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.base.Preconditions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;

/** */
public class Carrot2ClusteringActionResponse extends ActionResponse implements ToXContent {
    static final class Fields {
        static final XContentBuilderString SEARCH_RESPONSE = new XContentBuilderString("search_response");
        static final XContentBuilderString CLUSTERS = new XContentBuilderString("clusters");
    }

    private SearchResponse searchResponse;
    private List<Cluster> clusters;

    Carrot2ClusteringActionResponse() {
    }

    public Carrot2ClusteringActionResponse(SearchResponse searchResponse, 
            List<Cluster> clusters) {
        this.searchResponse = Preconditions.checkNotNull(searchResponse);
        this.clusters = Preconditions.checkNotNull(clusters);
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public List<Cluster> getClusters() {
        return clusters;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params)
            throws IOException {
        if (searchResponse != null) {
            searchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }

        builder.startArray(Fields.CLUSTERS);
        if (clusters != null) {
            appendToXContent(builder, clusters);
        }
        builder.endArray();
        return builder;
    }

    private void appendToXContent(XContentBuilder builder, List<Cluster> clusters) throws IOException {
        for (Cluster c : clusters) {
            builder.startObject();
            builder.field("id", c.getId());
            builder.field("label", c.getLabel());
            builder.field("score", c.getScore());
            builder.array("phrases", c.getPhrases().toArray(new String [c.getPhrases().size()]));
            if (c.isOtherTopics()) {
                builder.field("other_topics", true);
            }

            List<Document> docs = c.getDocuments();
            builder.startArray("documents");
            for (Document doc : docs) {
                builder.startObject();
                builder.field("id", doc.getStringId());
                builder.endObject();
            }
            builder.endArray();

            List<Cluster> subclusters = c.getSubclusters();
            if (subclusters != null && !subclusters.isEmpty()) {
                builder.startArray("clusters");
                appendToXContent(builder, subclusters);
                builder.endArray();
            }

            builder.endObject();
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
    }
    
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        
        boolean hasSearchResponse = searchResponse != null;
        out.writeBoolean(hasSearchResponse);
        if (hasSearchResponse) {
            this.searchResponse.writeTo(out);
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
