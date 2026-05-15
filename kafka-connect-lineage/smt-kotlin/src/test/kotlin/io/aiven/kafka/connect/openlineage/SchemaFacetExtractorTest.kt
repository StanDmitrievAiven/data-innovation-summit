package io.aiven.kafka.connect.openlineage

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class SchemaFacetExtractorTest {

    @Test
    fun `extractFields from struct schema`() {
        val schema = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("email", Schema.STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .build()

        val fields = SchemaFacetExtractor.extractFields(schema)

        assertEquals(4, fields.size)
        assertEquals("id", fields[0]["name"])
        assertEquals("INT32", fields[0]["type"])
        assertEquals("name", fields[1]["name"])
        assertEquals("STRING", fields[1]["type"])
        assertEquals("email", fields[2]["name"])
        assertEquals("STRING", fields[2]["type"])
        assertEquals("age", fields[3]["name"])
        assertEquals("INT32", fields[3]["type"])
    }

    @Test
    fun `extractFields from Debezium envelope`() {
        val innerSchema = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("amount", Schema.FLOAT64_SCHEMA)
            .build()

        val envelope = SchemaBuilder.struct()
            .field("before", innerSchema)
            .field("after", innerSchema)
            .field("source", SchemaBuilder.struct().field("version", Schema.STRING_SCHEMA).build())
            .field("op", Schema.STRING_SCHEMA)
            .build()

        val fields = SchemaFacetExtractor.extractFields(envelope)

        assertEquals(3, fields.size)
        assertEquals("id", fields[0]["name"])
        assertEquals("INT32", fields[0]["type"])
        assertEquals("name", fields[1]["name"])
        assertEquals("STRING", fields[1]["type"])
        assertEquals("amount", fields[2]["name"])
        assertEquals("FLOAT64", fields[2]["type"])
    }

    @Test
    fun `extractFields from null schema returns empty`() {
        assertTrue(SchemaFacetExtractor.extractFields(null).isEmpty())
    }

    @Test
    fun `extractFields from primitive schema`() {
        val fields = SchemaFacetExtractor.extractFields(Schema.STRING_SCHEMA)

        assertEquals(1, fields.size)
        assertEquals("value", fields[0]["name"])
        assertEquals("STRING", fields[0]["type"])
    }

    @Test
    fun `mapConnectType for all types`() {
        assertEquals("INT8", SchemaFacetExtractor.mapConnectType(Schema.INT8_SCHEMA))
        assertEquals("INT16", SchemaFacetExtractor.mapConnectType(Schema.INT16_SCHEMA))
        assertEquals("INT32", SchemaFacetExtractor.mapConnectType(Schema.INT32_SCHEMA))
        assertEquals("INT64", SchemaFacetExtractor.mapConnectType(Schema.INT64_SCHEMA))
        assertEquals("FLOAT32", SchemaFacetExtractor.mapConnectType(Schema.FLOAT32_SCHEMA))
        assertEquals("FLOAT64", SchemaFacetExtractor.mapConnectType(Schema.FLOAT64_SCHEMA))
        assertEquals("BOOLEAN", SchemaFacetExtractor.mapConnectType(Schema.BOOLEAN_SCHEMA))
        assertEquals("STRING", SchemaFacetExtractor.mapConnectType(Schema.STRING_SCHEMA))
        assertEquals("BYTES", SchemaFacetExtractor.mapConnectType(Schema.BYTES_SCHEMA))
        assertEquals("UNKNOWN", SchemaFacetExtractor.mapConnectType(null))
    }

    @Test
    fun `mapConnectType for logical types`() {
        val decimalSchema = SchemaBuilder.bytes().name("org.apache.kafka.connect.data.Decimal").build()
        assertEquals("DECIMAL", SchemaFacetExtractor.mapConnectType(decimalSchema))

        val dateSchema = SchemaBuilder.int32().name("org.apache.kafka.connect.data.Date").build()
        assertEquals("DATE", SchemaFacetExtractor.mapConnectType(dateSchema))

        val timestampSchema = SchemaBuilder.int64().name("org.apache.kafka.connect.data.Timestamp").build()
        assertEquals("TIMESTAMP", SchemaFacetExtractor.mapConnectType(timestampSchema))
    }

    @Test
    fun `unwrapDebeziumEnvelope for non-Debezium returns original`() {
        val plainStruct = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build()

        assertSame(plainStruct, SchemaFacetExtractor.unwrapDebeziumEnvelope(plainStruct))
    }
}
