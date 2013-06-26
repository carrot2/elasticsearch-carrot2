package org.carrot2.elasticsearch.plugin;

enum FieldSource {
    HIGHLIGHT,
    FIELD;

    static FieldSource [] cachedByOrdinal = values();
    static FieldSource fromOrdinal(int ordinal) {
        return cachedByOrdinal[ordinal];
    }
}