package org.carrot2.elasticsearch;

/** The source of data for a logical document field. */
enum FieldSource {
  HIGHLIGHT("highlight."),
  FIELD("fields."),
  SOURCE("_source.");

  /** Field specification prefix for this source. */
  private final String fieldSpecPrefix;

  static class ParsedFieldSource {
    final FieldSource source;
    final String fieldName;

    ParsedFieldSource(FieldSource source, String fieldName) {
      this.source = source;
      this.fieldName = fieldName;
    }
  }

  static ParsedFieldSource parseSpec(String fieldSourceSpec) {
    if (fieldSourceSpec != null) {
      for (FieldSource fs : cachedByOrdinal) {
        if (fieldSourceSpec.startsWith(fs.fieldSpecPrefix)) {
          return new ParsedFieldSource(fs, fieldSourceSpec.substring(fs.fieldSpecPrefix.length()));
        }
      }
    }
    return null;
  }

  static FieldSource[] cachedByOrdinal = values();

  static FieldSource fromOrdinal(int ordinal) {
    return cachedByOrdinal[ordinal];
  }

  FieldSource(String fieldSpecPrefix) {
    this.fieldSpecPrefix = fieldSpecPrefix;
  }
}
