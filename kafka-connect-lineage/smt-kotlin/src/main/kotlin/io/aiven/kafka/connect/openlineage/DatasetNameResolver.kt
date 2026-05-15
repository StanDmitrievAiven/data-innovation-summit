package io.aiven.kafka.connect.openlineage

/**
 * Resolves OpenLineage dataset namespace and name from connector configuration and record metadata.
 *
 * The core SMT logic (`OpenLineageSmt`, `OpenLineageEventBuilder`) is connector-agnostic.
 * This class contains the only connector-specific logic: translating Debezium's topic naming
 * convention (`prefix.schema.table`) into clean dataset names (`schema.table`). Without this,
 * dataset names would include the Debezium topic prefix, which is not meaningful as a dataset
 * identifier. For non-Debezium connectors, the topic name is used as-is or combined with a
 * configured prefix.
 */
object DatasetNameResolver {

    /** Builds the OpenLineage namespace for a Kafka topic. */
    fun kafkaNamespace(bootstrapServers: String): String = "kafka://$bootstrapServers"

    /**
     * Resolves the source (non-Kafka) dataset namespace.
     *
     * If explicitly configured via `source.namespace`, uses that.
     * Otherwise, falls back to the job namespace.
     */
    fun resolveSourceNamespace(config: OpenLineageSmtConfig): String =
        config.sourceNamespace().ifEmpty { config.namespace() }

    /** Resolves the sink (non-Kafka) dataset namespace. */
    fun resolveSinkNamespace(config: OpenLineageSmtConfig): String =
        config.sinkNamespace().ifEmpty { config.namespace() }

    /**
     * Resolves the source dataset name from a Kafka topic name.
     *
     * For Debezium connectors, the topic is typically `prefix.schema.table`, so we
     * extract the last two segments as the dataset name (e.g., `public.customers`).
     *
     * For other connectors, uses the source dataset prefix + topic-derived table name.
     */
    fun resolveSourceDatasetName(config: OpenLineageSmtConfig, topic: String): String {
        val connectorClass = config.connectorClass()
        val prefix = config.sourceDatasetPrefix()
        val database = config.sourceDatasetDatabase()

        val baseName = if (isDebezium(connectorClass)) {
            extractDebeziumTableName(topic)
        } else if (prefix.isNotEmpty()) {
            if (topic.contains(".")) topic.substringAfter(".")
            else "$prefix.$topic"
        } else {
            topic
        }

        return if (database.isNotEmpty()) "$database.$baseName" else baseName
    }

    /**
     * Resolves the sink dataset name from a Kafka topic name.
     *
     * For JDBC sink: Debezium-style topics like `prefix.schema.table` map to `schema.table`.
     * If a database qualifier is configured, prepends it (e.g., `analytics.public.users`).
     */
    fun resolveSinkDatasetName(config: OpenLineageSmtConfig, topic: String): String {
        val database = config.sinkDatasetDatabase()
        val parts = topic.split(".")
        val baseName = when {
            parts.size >= 3 -> "${parts[parts.size - 2]}.${parts.last()}"
            else -> topic
        }
        return if (database.isNotEmpty()) "$database.$baseName" else baseName
    }

    /**
     * Extracts the table name from a Debezium-style topic name.
     */
    internal fun extractDebeziumTableName(topic: String): String {
        val parts = topic.split(".")
        return when {
            parts.size >= 3 -> "${parts[parts.size - 2]}.${parts.last()}"
            else -> topic
        }
    }

    /** Checks if the connector class is a Debezium connector. */
    internal fun isDebezium(connectorClass: String): Boolean =
        connectorClass.startsWith("io.debezium.")

    /** Checks if the connector class is a sink connector (heuristic: name contains "Sink"). */
    fun isSinkConnector(connectorClass: String): Boolean =
        connectorClass.lowercase().contains("sink")
}
