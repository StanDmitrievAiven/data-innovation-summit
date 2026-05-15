package io.aiven.kafka.connect.openlineage;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Extracts OpenLineage {@code SchemaDatasetFacet} from a Kafka Connect {@link Schema}.
 *
 * <p>For most connectors, the record schema directly contains the data fields ({@code id}, {@code name}, etc.).
 * Debezium is a special case: it wraps records in an envelope with {@code before}, {@code after}, {@code source},
 * and {@code op} fields. The actual table columns live inside the {@code after} struct. This extractor
 * detects the Debezium envelope pattern (presence of both {@code before} and {@code after} struct fields)
 * and extracts from {@code after} automatically. Without this, lineage would report envelope field
 * names instead of actual column names.</p>
 */
public final class SchemaFacetExtractor {

    private SchemaFacetExtractor() {
    }

    /**
     * Extract field descriptors from a Connect schema.
     *
     * @param schema the value schema from a Connect record
     * @return list of field maps with "name" and "type" keys
     */
    public static List<Map<String, String>> extractFields(final Schema schema) {
        if (schema == null) {
            return List.of();
        }

        // Debezium envelope: look for "after" struct
        final Schema effectiveSchema = unwrapDebeziumEnvelope(schema);

        if (effectiveSchema.type() != Schema.Type.STRUCT) {
            // Primitive schema — single unnamed field
            final List<Map<String, String>> result = new ArrayList<>();
            final Map<String, String> field = new LinkedHashMap<>();
            field.put("name", "value");
            field.put("type", mapConnectType(effectiveSchema));
            result.add(field);
            return result;
        }

        final List<Map<String, String>> fields = new ArrayList<>();
        for (final Field f : effectiveSchema.fields()) {
            final Map<String, String> fieldMap = new LinkedHashMap<>();
            fieldMap.put("name", f.name());
            fieldMap.put("type", mapConnectType(f.schema()));
            fields.add(fieldMap);
        }
        return fields;
    }

    /**
     * Get the field names from a schema (for column lineage mapping).
     */
    public static List<String> fieldNames(final Schema schema) {
        final List<Map<String, String>> fields = extractFields(schema);
        final List<String> names = new ArrayList<>();
        for (final Map<String, String> f : fields) {
            names.add(f.get("name"));
        }
        return names;
    }

    /**
     * If the schema looks like a Debezium envelope (has "before" and "after" struct fields),
     * return the "after" schema. Otherwise return the schema as-is.
     */
    static Schema unwrapDebeziumEnvelope(final Schema schema) {
        if (schema == null || schema.type() != Schema.Type.STRUCT) {
            return schema;
        }

        final Field afterField = schema.field("after");
        if (afterField != null && afterField.schema().type() == Schema.Type.STRUCT) {
            // Also check for "before" field to confirm Debezium envelope
            final Field beforeField = schema.field("before");
            if (beforeField != null) {
                return afterField.schema();
            }
        }
        return schema;
    }

    /**
     * Map a Kafka Connect schema type to an OpenLineage-friendly type string.
     */
    static String mapConnectType(final Schema schema) {
        if (schema == null) {
            return "UNKNOWN";
        }

        // Check for logical types first
        if (schema.name() != null) {
            switch (schema.name()) {
                case "org.apache.kafka.connect.data.Decimal":
                    return "DECIMAL";
                case "org.apache.kafka.connect.data.Date":
                    return "DATE";
                case "org.apache.kafka.connect.data.Time":
                    return "TIME";
                case "org.apache.kafka.connect.data.Timestamp":
                    return "TIMESTAMP";
                case "io.debezium.data.VariableScaleDecimal":
                    return "DECIMAL";
                default:
                    break;
            }
        }

        switch (schema.type()) {
            case INT8:
                return "INT8";
            case INT16:
                return "INT16";
            case INT32:
                return "INT32";
            case INT64:
                return "INT64";
            case FLOAT32:
                return "FLOAT32";
            case FLOAT64:
                return "FLOAT64";
            case BOOLEAN:
                return "BOOLEAN";
            case STRING:
                return "STRING";
            case BYTES:
                return "BYTES";
            case ARRAY:
                return "ARRAY";
            case MAP:
                return "MAP";
            case STRUCT:
                return "STRUCT";
            default:
                return "UNKNOWN";
        }
    }
}
