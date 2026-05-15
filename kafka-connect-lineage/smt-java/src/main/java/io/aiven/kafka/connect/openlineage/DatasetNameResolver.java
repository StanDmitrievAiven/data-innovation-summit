package io.aiven.kafka.connect.openlineage;

/**
 * Resolves OpenLineage dataset namespace and name from connector configuration and record metadata.
 *
 * <p>The core SMT logic ({@code OpenLineageSmt}, {@code OpenLineageEventBuilder}) is connector-agnostic.
 * This class contains the only connector-specific logic: translating Debezium's topic naming
 * convention ({@code prefix.schema.table}) into clean dataset names ({@code schema.table}). Without this,
 * dataset names would include the Debezium topic prefix, which is not meaningful as a dataset
 * identifier. For non-Debezium connectors, the topic name is used as-is or combined with a
 * configured prefix.</p>
 */
public final class DatasetNameResolver {

    private DatasetNameResolver() {
    }

    /**
     * Determine the OpenLineage namespace for a Kafka topic.
     */
    public static String kafkaNamespace(final String bootstrapServers) {
        return "kafka://" + bootstrapServers;
    }

    /**
     * Resolve the source (non-Kafka) dataset namespace.
     *
     * <p>If explicitly configured via {@code source.namespace}, uses that.
     * Otherwise attempts to derive from the connector class.</p>
     */
    public static String resolveSourceNamespace(final OpenLineageSmtConfig config) {
        final String explicit = config.sourceNamespace();
        if (explicit != null && !explicit.isEmpty()) {
            return explicit;
        }
        // Fallback: use namespace config as prefix
        return config.namespace();
    }

    /**
     * Resolve the sink (non-Kafka) dataset namespace.
     */
    public static String resolveSinkNamespace(final OpenLineageSmtConfig config) {
        final String explicit = config.sinkNamespace();
        if (explicit != null && !explicit.isEmpty()) {
            return explicit;
        }
        return config.namespace();
    }

    /**
     * Resolve the source dataset name from a Kafka topic name.
     *
     * <p>For Debezium connectors, the topic is typically {@code prefix.schema.table}, so we
     * extract the last two segments as the dataset name (e.g., {@code public.customers}).</p>
     *
     * <p>For other connectors, uses the source dataset prefix + topic-derived table name.</p>
     */
    public static String resolveSourceDatasetName(
            final OpenLineageSmtConfig config,
            final String topic) {

        final String connectorClass = config.connectorClass();
        final String prefix = config.sourceDatasetPrefix();

        // Debezium: topic = "prefix.schema.table" to dataset = "schema.table"
        if (isDebezium(connectorClass)) {
            return extractDebeziumTableName(topic);
        }

        // JDBC source: use prefix.topic or just topic
        if (prefix != null && !prefix.isEmpty()) {
            // If topic already starts with prefix, use as-is
            if (topic.contains(".")) {
                return topic.substring(topic.indexOf('.') + 1);
            }
            return prefix + "." + topic;
        }

        return topic;
    }

    /**
     * Resolve the sink dataset name from a Kafka topic name.
     *
     * <p>For JDBC sink: topic name maps to table name. Debezium-style topics like
     * {@code prefix.schema.table} map to {@code schema.table}.</p>
     */
    public static String resolveSinkDatasetName(final String topic) {
        // If the topic has a prefix like "inventory.public.customers",
        // use the last two segments as table name
        final String[] parts = topic.split("\\.");
        if (parts.length >= 3) {
            return parts[parts.length - 2] + "." + parts[parts.length - 1];
        }
        if (parts.length == 2) {
            return topic;
        }
        return topic;
    }

    /**
     * Extract table name from a Debezium-style topic name.
     *
     * <p>{@code "inventory.public.customers"} to {@code "public.customers"}</p>
     */
    static String extractDebeziumTableName(final String topic) {
        final String[] parts = topic.split("\\.");
        if (parts.length >= 3) {
            // prefix.schema.table to schema.table
            return parts[parts.length - 2] + "." + parts[parts.length - 1];
        }
        if (parts.length == 2) {
            return topic;
        }
        return topic;
    }

    /**
     * Check if the connector class is a Debezium connector.
     */
    static boolean isDebezium(final String connectorClass) {
        return connectorClass != null && connectorClass.startsWith("io.debezium.");
    }

    /**
     * Check if the connector class is a sink connector (heuristic: class name contains "Sink").
     */
    public static boolean isSinkConnector(final String connectorClass) {
        return connectorClass != null && connectorClass.toLowerCase().contains("sink");
    }
}
