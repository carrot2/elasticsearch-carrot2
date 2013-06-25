package org.carrot2.elasticsearch.plugin;

import java.io.IOException;

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

    public Carrot2ClusteringActionResponse() {
    }

    public Carrot2ClusteringActionResponse(SearchResponse searchResponse) {
        this.searchResponse = Preconditions.checkNotNull(searchResponse);
    }

    public void setSearchResponse(SearchResponse searchResponse) {
        this.searchResponse = searchResponse;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params)
            throws IOException {
        if (searchResponse != null) {
            searchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.startObject(Fields.CLUSTERS);
            builder.field("foo", "bar");
            builder.endObject();
        }
        return builder;
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
