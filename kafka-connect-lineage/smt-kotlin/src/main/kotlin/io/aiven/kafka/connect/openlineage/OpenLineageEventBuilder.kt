package io.aiven.kafka.connect.openlineage

import org.apache.kafka.connect.data.Schema
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * Builds OpenLineage [RunEvent](https://openlineage.io/spec) JSON structures
 * from Kafka Connect record metadata.
 *
 * Produces events conforming to the OpenLineage spec, including:
 * - `SchemaDatasetFacet` — field names and types
 * - `ColumnLineageDatasetFacet` — column-to-column mapping
 *
 * Each topic gets its own runId so that the lineage backend creates separate lineage edges
 * per topic (input dataset to job to output dataset).
 *
 * @param config the SMT configuration
 */
class OpenLineageEventBuilder(private val config: OpenLineageSmtConfig) {

    private val runIdsByTopic = ConcurrentHashMap<String, String>()

    /**
     * Gets or creates a stable runId for a given topic.
     * Each topic gets its own run so the lineage backend builds correct per-topic graphs.
     */
    internal fun runIdForTopic(topic: String): String =
        runIdsByTopic.computeIfAbsent(topic) { UUID.randomUUID().toString() }

    /** Returns all tracked topic runIds (for emitting COMPLETE events on close). */
    val trackedRunIds: Map<String, String>
        get() = runIdsByTopic.toMap()

    /**
     * Builds a lifecycle event (START, COMPLETE, FAIL) without schema information.
     *
     * @param eventType START, COMPLETE, or FAIL
     * @param jobName the job name (should match the RUNNING event job name)
     * @param topic the topic (used to look up the per-topic runId)
     */
    fun buildLifecycleEvent(
        eventType: String,
        jobName: String,
        topic: String,
    ): MutableMap<String, Any> = linkedMapOf(
        "eventType" to eventType,
        "eventTime" to ISO_FMT.format(Instant.now()),
        "producer" to PRODUCER,
        "schemaURL" to SCHEMA_URL,
        "run" to buildRun(topic),
        "job" to buildJob(jobName),
        "inputs" to emptyList<Any>(),
        "outputs" to emptyList<Any>(),
    )

    /**
     * Builds a RUNNING event for a source connector (source system to Kafka) with column-level lineage.
     *
     * @param jobName the connector + task name (e.g., `inventory-source.0`)
     * @param topic the Kafka topic name
     * @param valueSchema the value schema from the Connect record
     */
    fun buildSourceRunningEvent(
        jobName: String,
        topic: String,
        valueSchema: Schema,
    ): MutableMap<String, Any> {
        val fields = SchemaFacetExtractor.extractFields(valueSchema)
        val fieldNames = SchemaFacetExtractor.fieldNames(valueSchema)

        val sourceNs = DatasetNameResolver.resolveSourceNamespace(config)
        val sourceDataset = DatasetNameResolver.resolveSourceDatasetName(config, topic)
        val kafkaNs = DatasetNameResolver.kafkaNamespace(config.kafkaBootstrapServers())

        val input = buildDataset(sourceNs, sourceDataset, fields, columnLineageFacet = null)
        val columnLineage = ColumnLineageFacetBuilder.buildForSource(fieldNames, sourceNs, sourceDataset)
        val output = buildDataset(kafkaNs, topic, fields, columnLineage)

        return linkedMapOf(
            "eventType" to "RUNNING",
            "eventTime" to ISO_FMT.format(Instant.now()),
            "producer" to PRODUCER,
            "schemaURL" to SCHEMA_URL,
            "run" to buildRun(topic),
            "job" to buildJob(jobName),
            "inputs" to listOf(input),
            "outputs" to listOf(output),
        )
    }

    /**
     * Builds a RUNNING event for a sink connector (Kafka to sink system) with column-level lineage.
     *
     * @param jobName the connector + task name (e.g., `inventory-jdbc-sink.0`)
     * @param topic the Kafka topic name
     * @param valueSchema the value schema from the Connect record
     */
    fun buildSinkRunningEvent(
        jobName: String,
        topic: String,
        valueSchema: Schema,
    ): MutableMap<String, Any> {
        val fields = SchemaFacetExtractor.extractFields(valueSchema)
        val fieldNames = SchemaFacetExtractor.fieldNames(valueSchema)

        val sinkNs = DatasetNameResolver.resolveSinkNamespace(config)
        val sinkDataset = DatasetNameResolver.resolveSinkDatasetName(config, topic)
        val kafkaNs = DatasetNameResolver.kafkaNamespace(config.kafkaBootstrapServers())

        val input = buildDataset(kafkaNs, topic, fields, columnLineageFacet = null)
        val columnLineage = ColumnLineageFacetBuilder.buildForSink(fieldNames, kafkaNs, topic)
        val output = buildDataset(sinkNs, sinkDataset, fields, columnLineage)

        return linkedMapOf(
            "eventType" to "RUNNING",
            "eventTime" to ISO_FMT.format(Instant.now()),
            "producer" to PRODUCER,
            "schemaURL" to SCHEMA_URL,
            "run" to buildRun(topic),
            "job" to buildJob(jobName),
            "inputs" to listOf(input),
            "outputs" to listOf(output),
        )
    }

    private fun buildRun(topic: String): Map<String, Any> =
        mapOf("runId" to runIdForTopic(topic))

    private fun buildJob(jobName: String): Map<String, Any> =
        mapOf("namespace" to config.namespace(), "name" to jobName)

    private fun buildDataset(
        namespace: String,
        name: String,
        fields: List<Map<String, String>>,
        columnLineageFacet: Map<String, Any>?,
    ): Map<String, Any> {
        val facets = linkedMapOf<String, Any>(
            "schema" to linkedMapOf(
                "_producer" to PRODUCER,
                "_schemaURL" to "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json",
                "fields" to fields,
            )
        )

        if (columnLineageFacet != null) {
            facets["columnLineage"] = columnLineageFacet
        }

        return linkedMapOf(
            "namespace" to namespace,
            "name" to name,
            "facets" to facets,
        )
    }

    companion object {
        private const val PRODUCER =
            "https://github.com/OpenLineage/OpenLineage/tree/1.24.2/integration/kafka"
        private const val SCHEMA_URL =
            "https://openlineage.io/spec/2-0-2/OpenLineage.json#/\$defs/RunEvent"

        private val ISO_FMT: DateTimeFormatter =
            DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC)
    }
}
