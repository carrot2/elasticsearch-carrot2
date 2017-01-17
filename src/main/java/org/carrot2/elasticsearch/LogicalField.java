package org.carrot2.elasticsearch;

import java.util.HashMap;
import java.util.Locale;

import org.carrot2.elasticsearch.ClusteringAction.ClusteringActionRequest;

/**
 * Logical fields of a document to be clustered.
 * 
 * @see ClusteringActionRequest#addFieldMappingSpec(String, LogicalField)
 * @see ClusteringActionRequest#addFieldMapping(String, LogicalField)
 * @see ClusteringActionRequest#addHighlightedFieldMapping(String, LogicalField)
 * @see ClusteringActionRequest#addSourceFieldMapping(String, LogicalField)
 */
public enum LogicalField {
    URL,
    TITLE,
    CONTENT,
    LANGUAGE;

    static final LogicalField [] cachedByOrdinal = values();
    static LogicalField fromOrdinal(int ordinal) {
        return cachedByOrdinal[ordinal];
    }

    static final HashMap<String,LogicalField> aliases;
    static {
        aliases = new HashMap<>();
        for (LogicalField v : LogicalField.values()) {
            aliases.put(v.name(), v);
            aliases.put(v.name().toLowerCase(Locale.ROOT), v);
        }
    }

    /**
     * Same as {@link LogicalField#valueOf(String)} but does not throw
     * an exception on invalid values (returns null).
     */
    static LogicalField valueOfCaseInsensitive(String enumValue) {
        return aliases.get(enumValue);
    }
}
