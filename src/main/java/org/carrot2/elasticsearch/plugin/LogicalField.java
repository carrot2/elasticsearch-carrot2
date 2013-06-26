package org.carrot2.elasticsearch.plugin;

/**
 * Logical fields of a document to be clustered.
 * 
 * @see Carrot2ClusteringActionRequest#addFieldTo(String, LogicalField)
 * @see Carrot2ClusteringActionRequest#addHighlightFieldTo(String, LogicalField)
 */
public enum LogicalField {
    URL,
    TITLE,
    CONTENT;

    static LogicalField [] cachedByOrdinal = values();
    static LogicalField fromOrdinal(int ordinal) {
        return cachedByOrdinal[ordinal];
    }
}
