package org.carrot2.elasticsearch.plugin;

import static org.elasticsearch.action.ValidateActions.addValidationError;

import java.io.IOException;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

/** */
public class Carrot2ClusteringActionRequest extends ActionRequest<Carrot2ClusteringActionRequest> {
    private SearchRequest searchRequest;

    public Carrot2ClusteringActionRequest setSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
        return this;
    }

    public Carrot2ClusteringActionRequest setSearchRequest(SearchRequestBuilder builder) {
        return setSearchRequest(builder.request());
    }

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (searchRequest == null) {
            validationException = addValidationError("No delegate search request", validationException);
        }
        
        ActionRequestValidationException ex = searchRequest.validate();
        if (ex != null) {
            if (validationException == null) {
                validationException = new ActionRequestValidationException();
            }
            validationException.addValidationErrors(ex.validationErrors());
        }
        
        return validationException;
    }
    
    @Override
    public void readFrom(StreamInput in) throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.readFrom(in);
        this.searchRequest = searchRequest;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert searchRequest != null;
        this.searchRequest.writeTo(out);
    }
}
