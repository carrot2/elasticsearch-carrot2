package org.carrot2.elasticsearch;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.carrot2.clustering.Cluster;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

/**
 * A {@link DocumentGroup} acts as an adapter over {@link Cluster}, providing additional
 * serialization methods and only exposing a subset of {@link Cluster}'s data.
 */
public class DocumentGroup implements ToXContent, Writeable {
  private static final DocumentGroup[] EMPTY_DOC_GROUP = new DocumentGroup[0];
  private static final String[] EMPTY_STRING_ARRAY = new String[0];

  private int id;
  private String[] phrases = EMPTY_STRING_ARRAY;
  private double score;
  private String[] documentReferences = EMPTY_STRING_ARRAY;
  private DocumentGroup[] subgroups = EMPTY_DOC_GROUP;
  private boolean ungroupedDocuments;
  private Set<String> uniqueDocuments;

  public DocumentGroup() {}

  DocumentGroup(StreamInput in) throws IOException {
    id = in.readVInt();
    score = in.readDouble();
    phrases = in.readStringArray();
    ungroupedDocuments = in.readBoolean();
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

  public String getLabel() {
    return String.join(", ", getPhrases());
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

  public void setUngroupedDocuments(boolean ungroupedDocuments) {
    this.ungroupedDocuments = ungroupedDocuments;
  }

  public boolean isUngroupedDocuments() {
    return ungroupedDocuments;
  }

  public Set<String> uniqueDocuments() {
    // Compute lazily.
    if (uniqueDocuments == null) {
      uniqueDocuments = new HashSet<>();
      uniqueDocuments.addAll(Arrays.asList(getDocumentReferences()));
      for (DocumentGroup group : subgroups) {
        uniqueDocuments.addAll(group.uniqueDocuments);
      }
    }
    return uniqueDocuments;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeVInt(id);
    out.writeDouble(score);
    out.writeStringArray(phrases);
    out.writeBoolean(ungroupedDocuments);
    out.writeStringArray(documentReferences);

    out.writeVInt(subgroups.length);
    for (DocumentGroup group : subgroups) {
      group.writeTo(out);
    }
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder.startObject();
    builder
        .field("id", id)
        .field("score", score)
        .field("label", getLabel())
        .array("phrases", phrases);

    if (ungroupedDocuments) {
      builder.field("other_topics", ungroupedDocuments);
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
