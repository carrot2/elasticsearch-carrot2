package org.carrot2.elasticsearch;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

class FieldMappingSpec implements Streamable {
    String field;
    LogicalField logicalField;
    FieldSource source;

    FieldMappingSpec(String field, LogicalField logicalField, FieldSource source) {
        this.field = field;
        this.logicalField = logicalField;
        this.source = source;
    }
    
    FieldMappingSpec() {}

    @Override
    public void readFrom(StreamInput in) throws IOException {
        field = in.readString();
        logicalField = LogicalField.fromOrdinal(in.readVInt());
        source = FieldSource.fromOrdinal(in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeVInt(logicalField.ordinal());
        out.writeVInt(source.ordinal());
    }
}