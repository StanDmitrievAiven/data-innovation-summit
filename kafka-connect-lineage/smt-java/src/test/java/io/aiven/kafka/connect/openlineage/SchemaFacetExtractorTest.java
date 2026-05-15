package io.aiven.kafka.connect.openlineage;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SchemaFacetExtractorTest {

    @Test
    void extractFields_fromStructSchema() {
        final Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("email", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();

        final List<Map<String, String>> fields = SchemaFacetExtractor.extractFields(schema);

        assertEquals(4, fields.size());
        assertEquals("id", fields.get(0).get("name"));
        assertEquals("INT32", fields.get(0).get("type"));
        assertEquals("name", fields.get(1).get("name"));
        assertEquals("STRING", fields.get(1).get("type"));
        assertEquals("email", fields.get(2).get("name"));
        assertEquals("STRING", fields.get(2).get("type"));
        assertEquals("age", fields.get(3).get("name"));
        assertEquals("INT32", fields.get(3).get("type"));
    }

    @Test
    void extractFields_fromDebeziumEnvelope() {
        final Schema innerSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("amount", Schema.FLOAT64_SCHEMA)
                .build();

        final Schema envelope = SchemaBuilder.struct()
                .field("before", innerSchema)
                .field("after", innerSchema)
                .field("source", SchemaBuilder.struct()
                        .field("version", Schema.STRING_SCHEMA)
                        .build())
                .field("op", Schema.STRING_SCHEMA)
                .build();

        final List<Map<String, String>> fields = SchemaFacetExtractor.extractFields(envelope);

        assertEquals(3, fields.size());
        assertEquals("id", fields.get(0).get("name"));
        assertEquals("INT32", fields.get(0).get("type"));
        assertEquals("name", fields.get(1).get("name"));
        assertEquals("STRING", fields.get(1).get("type"));
        assertEquals("amount", fields.get(2).get("name"));
        assertEquals("FLOAT64", fields.get(2).get("type"));
    }

    @Test
    void extractFields_fromNullSchema_returnsEmpty() {
        assertTrue(SchemaFacetExtractor.extractFields(null).isEmpty());
    }

    @Test
    void extractFields_fromPrimitiveSchema() {
        final List<Map<String, String>> fields = SchemaFacetExtractor.extractFields(Schema.STRING_SCHEMA);
        assertEquals(1, fields.size());
        assertEquals("value", fields.get(0).get("name"));
        assertEquals("STRING", fields.get(0).get("type"));
    }

    @Test
    void mapConnectType_allTypes() {
        assertEquals("INT8", SchemaFacetExtractor.mapConnectType(Schema.INT8_SCHEMA));
        assertEquals("INT16", SchemaFacetExtractor.mapConnectType(Schema.INT16_SCHEMA));
        assertEquals("INT32", SchemaFacetExtractor.mapConnectType(Schema.INT32_SCHEMA));
        assertEquals("INT64", SchemaFacetExtractor.mapConnectType(Schema.INT64_SCHEMA));
        assertEquals("FLOAT32", SchemaFacetExtractor.mapConnectType(Schema.FLOAT32_SCHEMA));
        assertEquals("FLOAT64", SchemaFacetExtractor.mapConnectType(Schema.FLOAT64_SCHEMA));
        assertEquals("BOOLEAN", SchemaFacetExtractor.mapConnectType(Schema.BOOLEAN_SCHEMA));
        assertEquals("STRING", SchemaFacetExtractor.mapConnectType(Schema.STRING_SCHEMA));
        assertEquals("BYTES", SchemaFacetExtractor.mapConnectType(Schema.BYTES_SCHEMA));
        assertEquals("UNKNOWN", SchemaFacetExtractor.mapConnectType(null));
    }

    @Test
    void mapConnectType_logicalTypes() {
        final Schema decimalSchema = SchemaBuilder.bytes()
                .name("org.apache.kafka.connect.data.Decimal")
                .build();
        assertEquals("DECIMAL", SchemaFacetExtractor.mapConnectType(decimalSchema));

        final Schema dateSchema = SchemaBuilder.int32()
                .name("org.apache.kafka.connect.data.Date")
                .build();
        assertEquals("DATE", SchemaFacetExtractor.mapConnectType(dateSchema));

        final Schema timestampSchema = SchemaBuilder.int64()
                .name("org.apache.kafka.connect.data.Timestamp")
                .build();
        assertEquals("TIMESTAMP", SchemaFacetExtractor.mapConnectType(timestampSchema));
    }

    @Test
    void unwrapDebeziumEnvelope_nonDebezium_returnsOriginal() {
        final Schema plainStruct = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();

        assertSame(plainStruct, SchemaFacetExtractor.unwrapDebeziumEnvelope(plainStruct));
    }

    @Test
    void unwrapDebeziumEnvelope_null_returnsNull() {
        assertNull(SchemaFacetExtractor.unwrapDebeziumEnvelope(null));
    }
}
