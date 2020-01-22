package org.carrot2.elasticsearch;

import org.carrot2.clustering.Cluster;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

/**
 * A {@link DocumentGroup} acts as an adapter over {@link Cluster}, providing additional
 * serialization methods and only exposing a subset of {@link Cluster}'s data.
 */
public class DocumentGroup implements ToXContent, Writeable {
    private static final DocumentGroup[] EMPTY_DOC_GROUP = new DocumentGroup[0];
    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    private int id;
    private String[] phrases = EMPTY_STRING_ARRAY;
    private String label;
    private double score;
    private String[] documentReferences = EMPTY_STRING_ARRAY;
    private DocumentGroup[] subgroups = EMPTY_DOC_GROUP;
    // TODO: remove other topics.
    private boolean otherTopics;

    public DocumentGroup() {
    }

    DocumentGroup(StreamInput in) throws IOException {
        id = in.readVInt();
        score = in.readDouble();
        label = in.readOptionalString();
        phrases = in.readStringArray();
        otherTopics = in.readBoolean();
        documentReferences = in.readStringArray();

        int max = in.readVInt();
        subgroups = new DocumentGroup[max];
        for (int i = 0; i < max; i++) {
            subgroups[i] = new DocumentGroup(in);
        }
    }

    public DocumentGroup[] getSubgroups() {
        return subgroups;
    }

    public void setSubgroups(DocumentGroup[] subclusters) {
        this.subgroups = Preconditions.checkNotNull(subclusters);
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setPhrases(String[] phrases) {
        this.phrases = Preconditions.checkNotNull(phrases);
    }

    public String[] getPhrases() {
        return phrases;
    }
    
    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return label;
    }
    
    public void setScore(Double score) {
        this.score = (score == null ? 0 : score);
    }
    
    public double getScore() {
        return score;
    }

    public void setDocumentReferences(String[] documentReferences) {
        this.documentReferences = Preconditions.checkNotNull(documentReferences);
    }
    
    public String[] getDocumentReferences() {
        return documentReferences;
    }

    public void setOtherTopics(boolean otherTopics) {
        this.otherTopics = otherTopics;
    }
    
    public boolean isOtherTopics() {
        return otherTopics;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(id);
        out.writeDouble(score);
        out.writeOptionalString(label);
        out.writeStringArray(phrases);
        out.writeBoolean(otherTopics);
        out.writeStringArray(documentReferences);
        
        out.writeVInt(subgroups.length);
        for (DocumentGroup group : subgroups) {
            group.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params)
            throws IOException {
        builder.startObject();
        builder
            .field("id", id)
            .field("score", score)
            .field("label", label)
            .array("phrases", phrases);
        
        if (otherTopics) {
            builder.field("other_topics", otherTopics);
        }

        if (documentReferences.length > 0) {
            builder.array("documents", documentReferences);
        }

        if (subgroups.length > 0) {
            builder.startArray("clusters");
            for (DocumentGroup group : subgroups) {
                group.toXContent(builder, params);
            }
            builder.endArray();
        }

        builder.endObject();
        return builder;
    }

    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            toXContent(builder, EMPTY_PARAMS);
            return Strings.toString(builder);
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
