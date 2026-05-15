package io.aiven.kafka.connect.openlineage

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.transforms.Transformation
import org.slf4j.LoggerFactory

/**
 * Kafka Connect Single Message Transform (SMT) that emits OpenLineage events
 * with column-level lineage for **any** connector.
 *
 * This SMT is a **pass-through** — it never modifies records. It observes the record
 * schema and emits OpenLineage events as a side effect.
 *
 * Each topic gets its own OpenLineage job + run, so the lineage backend builds a separate
 * lineage graph per topic: `input dataset to job to output dataset`.
 *
 * @param R the record type (SourceRecord or SinkRecord)
 */
class OpenLineageSmt<R : ConnectRecord<R>> : Transformation<R> {

    private lateinit var smtConfig: OpenLineageSmtConfig
    private lateinit var transport: OpenLineageTransport
    private lateinit var eventBuilder: OpenLineageEventBuilder

    private var isSink = false
    private val emittedTopics = mutableSetOf<String>()

    override fun configure(props: MutableMap<String, *>) {
        smtConfig = OpenLineageSmtConfig(props)
        transport = OpenLineageTransport(smtConfig.transportConfigPath(), smtConfig.transportType())
        eventBuilder = OpenLineageEventBuilder(smtConfig)
        isSink = DatasetNameResolver.isSinkConnector(smtConfig.connectorClass())

        log.info(
            "OpenLineage SMT configured: namespace={}, connector={}, isSink={}",
            smtConfig.namespace(), smtConfig.connectorClass(), isSink,
        )
    }

    override fun apply(record: R?): R? {
        if (record == null) return null

        runCatching {
            val topic = record.topic() ?: return@runCatching
            if (topic in emittedTopics) return@runCatching

            val valueSchema = record.valueSchema() ?: return@runCatching
            val jobName = resolveJobName(topic)

            // Emit START to RUNNING to COMPLETE for this topic's job.
            // The lineage backend needs a COMPLETE event with inputs/outputs to build graph edges.
            transport.emit(eventBuilder.buildLifecycleEvent("START", jobName, topic))
            emitWithIO("RUNNING", jobName, topic, valueSchema)
            emitWithIO("COMPLETE", jobName, topic, valueSchema)

            emittedTopics.add(topic)
            log.info(
                "OpenLineage events emitted for job={} topic={} (fields: {})",
                jobName, topic, SchemaFacetExtractor.fieldNames(valueSchema),
            )
        }.onFailure { e ->
            // Never fail the pipeline — log and continue
            log.warn("OpenLineage SMT error (non-fatal): {}", e.message, e)
        }

        return record
    }

    private fun emitWithIO(
        eventType: String,
        jobName: String,
        topic: String,
        valueSchema: org.apache.kafka.connect.data.Schema,
    ) {
        val event = if (isSink) {
            eventBuilder.buildSinkRunningEvent(jobName, topic, valueSchema)
        } else {
            eventBuilder.buildSourceRunningEvent(jobName, topic, valueSchema)
        }
        event["eventType"] = eventType
        transport.emit(event)
    }

    /**
     * Builds a per-topic job name. Each topic maps to its own OpenLineage job
     * so that lineage edges are 1:1 (one input to one job to one output).
     */
    private fun resolveJobName(topic: String): String {
        val prefix = smtConfig.connectorClass()
            .takeIf { it.isNotEmpty() }
            ?.substringAfterLast(".")
            ?: "connect"
        return "$prefix.$topic"
    }

    override fun config(): ConfigDef = OpenLineageSmtConfig.CONFIG_DEF

    override fun close() {
        // COMPLETE events are already sent per-topic in apply(). Nothing to do here.
    }

    companion object {
        private val log = LoggerFactory.getLogger(OpenLineageSmt::class.java)
    }
}
