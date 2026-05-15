package io.aiven.kafka.connect.openlineage

import org.apache.kafka.connect.data.Schema

/**
 * Extracts OpenLineage [SchemaDatasetFacet](https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json)
 * from a Kafka Connect [Schema].
 *
 * For most connectors, the record schema directly contains the data fields (`id`, `name`, etc.).
 * Debezium is a special case: it wraps records in an envelope with `before`, `after`, `source`,
 * and `op` fields. The actual table columns live inside the `after` struct. This extractor
 * detects the Debezium envelope pattern (presence of both `before` and `after` struct fields)
 * and extracts from `after` automatically. Without this, lineage would report envelope field
 * names instead of actual column names.
 */
object SchemaFacetExtractor {

    /**
     * Extracts field descriptors from a Connect schema.
     *
     * @param schema the value schema from a Connect record
     * @return list of field maps with `name` and `type` keys
     */
    fun extractFields(schema: Schema?): List<Map<String, String>> {
        if (schema == null) return emptyList()

        val effectiveSchema = unwrapDebeziumEnvelope(schema)

        if (effectiveSchema.type() != Schema.Type.STRUCT) {
            return listOf(mapOf("name" to "value", "type" to mapConnectType(effectiveSchema)))
        }

        return effectiveSchema.fields().map { field ->
            mapOf("name" to field.name(), "type" to mapConnectType(field.schema()))
        }
    }

    /** Returns the field names from a schema (for column lineage mapping). */
    fun fieldNames(schema: Schema?): List<String> =
        extractFields(schema).map { it.getValue("name") }

    /**
     * If the schema looks like a Debezium envelope (has `before` and `after` struct fields),
     * returns the `after` schema. Otherwise returns the schema as-is.
     */
    internal fun unwrapDebeziumEnvelope(schema: Schema): Schema {
        if (schema.type() != Schema.Type.STRUCT) return schema

        val afterField = schema.field("after") ?: return schema
        if (afterField.schema().type() != Schema.Type.STRUCT) return schema

        val beforeField = schema.field("before") ?: return schema
        return afterField.schema()
    }

    /** Maps a Kafka Connect schema type to an OpenLineage-friendly type string. */
    internal fun mapConnectType(schema: Schema?): String {
        if (schema == null) return "UNKNOWN"

        // Check for logical types first
        when (schema.name()) {
            "org.apache.kafka.connect.data.Decimal",
            "io.debezium.data.VariableScaleDecimal" -> return "DECIMAL"
            "org.apache.kafka.connect.data.Date" -> return "DATE"
            "org.apache.kafka.connect.data.Time" -> return "TIME"
            "org.apache.kafka.connect.data.Timestamp" -> return "TIMESTAMP"
        }

        return when (schema.type()) {
            Schema.Type.INT8 -> "INT8"
            Schema.Type.INT16 -> "INT16"
            Schema.Type.INT32 -> "INT32"
            Schema.Type.INT64 -> "INT64"
            Schema.Type.FLOAT32 -> "FLOAT32"
            Schema.Type.FLOAT64 -> "FLOAT64"
            Schema.Type.BOOLEAN -> "BOOLEAN"
            Schema.Type.STRING -> "STRING"
            Schema.Type.BYTES -> "BYTES"
            Schema.Type.ARRAY -> "ARRAY"
            Schema.Type.MAP -> "MAP"
            Schema.Type.STRUCT -> "STRUCT"
            else -> "UNKNOWN"
        }
    }
}
