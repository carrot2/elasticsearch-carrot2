package org.carrot2.elasticsearch.plugin;

import java.util.HashMap;

import org.elasticsearch.common.collect.Maps;

/**
 * Logical fields of a document to be clustered.
 * 
 * @see Carrot2ClusteringActionRequest#addFieldMapping(String, LogicalField)
 * @see Carrot2ClusteringActionRequest#addHighlightedFieldMapping(String, LogicalField)
 */
public enum LogicalField {
    URL,
    TITLE,
    CONTENT;

    static LogicalField [] cachedByOrdinal = values();
    static LogicalField fromOrdinal(int ordinal) {
        return cachedByOrdinal[ordinal];
    }

    static final HashMap<String,LogicalField> aliases;
    static {
        aliases = Maps.newHashMap();
        for (LogicalField v : LogicalField.values()) {
            aliases.put(v.name(), v);
            aliases.put(v.name().toLowerCase(), v);
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
