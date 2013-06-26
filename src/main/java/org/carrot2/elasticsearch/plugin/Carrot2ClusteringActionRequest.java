package org.carrot2.elasticsearch.plugin;

import static org.elasticsearch.action.ValidateActions.addValidationError;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

/** */
public class Carrot2ClusteringActionRequest extends ActionRequest<Carrot2ClusteringActionRequest> {
    private SearchRequest searchRequest;
    private String queryHint;
    private List<FieldMappingSpec> fieldMapping = Lists.newArrayList();

    /**
     * Set the {@link SearchRequest} to use for fetching documents to be clustered.
     * The search request must fetch enough documents for clustering to make sense
     * (set <code>size</code> appropriately).
     */
    public Carrot2ClusteringActionRequest setSearchRequest(SearchRequest searchRequest) {
        this.searchRequest = searchRequest;
        return this;
    }

    /**
     * @see #setSearchRequest(SearchRequest)
     */
    public Carrot2ClusteringActionRequest setSearchRequest(SearchRequestBuilder builder) {
        return setSearchRequest(builder.request());
    }

    public SearchRequest getSearchRequest() {
        return searchRequest;
    }

    /**
     * @param queryHint A set of terms which correspond to the query. This hint helps the
     *  clustering algorithm to avoid trivial clusters around the query terms. Typically the query
     *  terms hint will be identical to what the user typed in the search box.
     *  <p>
     *  The hint may be an empty string but must not be <code>null</code>.
     */
    public void setQueryHint(String queryHint) {
        this.queryHint = queryHint;
    }
    
    /**
     * @see #setQueryHint(String)
     */
    public String getQueryHint() {
        return queryHint;
    }

    /**
     * Map a hit's field to a logical section of a document to be clustered (title, content or URL).
     * @see LogicalField
     */
    public void addFieldTo(String fieldName, LogicalField logicalField) {
        fieldMapping.add(new FieldMappingSpec(fieldName, logicalField, FieldSource.FIELD));
    }

    /**
     * Map a hit's highligted field (fragments of the original field) to a logical section
     * of a document to be clustered. This may be used to decrease the amount of information
     * passed to the clustering engine but also to "focus" the clustering engine on the context
     * of the query.
     */
    public void addHighlightFieldTo(String fieldName, LogicalField logicalField) {
        fieldMapping.add(new FieldMappingSpec(fieldName, logicalField, FieldSource.HIGHLIGHT));
    }

    /** Access to prepared field mapping. */
    List<FieldMappingSpec> getFieldMapping() {
        return fieldMapping;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (searchRequest == null) {
            validationException = addValidationError("No delegate search request", validationException);
        }
        
        if (queryHint == null) {
            validationException = addValidationError("query hint may be empty but must not be null.", validationException);
        }
        
        if (fieldMapping.isEmpty()) {
            validationException = addValidationError("At least one field should be mapped to a logical document field.", validationException);
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
        this.queryHint = in.readOptionalString();
        int count = in.readVInt();
        while (count-- > 0) {
            FieldMappingSpec spec = new FieldMappingSpec();
            spec.readFrom(in);
            fieldMapping.add(spec);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert searchRequest != null;
        this.searchRequest.writeTo(out);
        
        out.writeOptionalString(queryHint);

        out.writeVInt(fieldMapping.size());
        for (FieldMappingSpec spec : fieldMapping) {
            spec.writeTo(out);
        }
    }
}
