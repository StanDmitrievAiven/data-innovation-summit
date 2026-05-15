package io.aiven.kafka.connect.openlineage;

import org.apache.kafka.connect.data.Schema;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Builds OpenLineage {@code RunEvent} JSON structures from Kafka Connect record metadata.
 *
 * <p>Produces events conforming to the OpenLineage spec at
 * <a href="https://openlineage.io/spec">openlineage.io/spec</a>, including:
 * <ul>
 *   <li>{@code SchemaDatasetFacet} — field names and types</li>
 *   <li>{@code ColumnLineageDatasetFacet} — column-to-column mapping</li>
 * </ul></p>
 *
 * <p>Each topic gets its own runId so that the lineage backend creates separate lineage edges
 * per topic (input dataset to job to output dataset).</p>
 */
public class OpenLineageEventBuilder {

    private static final String PRODUCER = "https://github.com/OpenLineage/OpenLineage/tree/1.24.2/integration/kafka";
    private static final String SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";
    private static final DateTimeFormatter ISO_FMT =
            DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);

    private final OpenLineageSmtConfig config;
    private final Map<String, String> runIdsByTopic = new ConcurrentHashMap<>();

    public OpenLineageEventBuilder(final OpenLineageSmtConfig config) {
        this.config = config;
    }

    /**
     * Get or create a stable runId for a given topic.
     * Each topic gets its own run so the lineage backend builds correct per-topic lineage graphs.
     */
    String runIdForTopic(final String topic) {
        return runIdsByTopic.computeIfAbsent(topic, k -> UUID.randomUUID().toString());
    }

    /**
     * Build a lifecycle event (START, COMPLETE, FAIL) without schema information.
     *
     * @param eventType START, COMPLETE, or FAIL
     * @param jobName   the job name (should match the RUNNING event job name)
     * @param topic     the topic (used to look up the per-topic runId)
     */
    public Map<String, Object> buildLifecycleEvent(
            final String eventType,
            final String jobName,
            final String topic) {

        final Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventType", eventType);
        event.put("eventTime", ISO_FMT.format(Instant.now()));
        event.put("producer", PRODUCER);
        event.put("schemaURL", SCHEMA_URL);
        event.put("run", buildRun(topic));
        event.put("job", buildJob(jobName));
        event.put("inputs", List.of());
        event.put("outputs", List.of());
        return event;
    }

    /**
     * Build a RUNNING event for a source connector (source system to Kafka) with column-level lineage.
     *
     * @param jobName      the connector + task name (e.g., "inventory-source.0")
     * @param topic        the Kafka topic name
     * @param valueSchema  the value schema from the Connect record
     */
    public Map<String, Object> buildSourceRunningEvent(
            final String jobName,
            final String topic,
            final Schema valueSchema) {

        final List<Map<String, String>> fields = SchemaFacetExtractor.extractFields(valueSchema);
        final List<String> fieldNames = SchemaFacetExtractor.fieldNames(valueSchema);

        final String sourceNs = DatasetNameResolver.resolveSourceNamespace(config);
        final String sourceDataset = DatasetNameResolver.resolveSourceDatasetName(config, topic);
        final String kafkaNs = DatasetNameResolver.kafkaNamespace(config.kafkaBootstrapServers());

        // Input: source dataset
        final Map<String, Object> input = buildDataset(sourceNs, sourceDataset, fields, null);

        // Output: Kafka topic with column lineage
        final Map<String, Object> columnLineage =
                ColumnLineageFacetBuilder.buildForSource(fieldNames, sourceNs, sourceDataset);
        final Map<String, Object> output = buildDataset(kafkaNs, topic, fields, columnLineage);

        final Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventType", "RUNNING");
        event.put("eventTime", ISO_FMT.format(Instant.now()));
        event.put("producer", PRODUCER);
        event.put("schemaURL", SCHEMA_URL);
        event.put("run", buildRun(topic));
        event.put("job", buildJob(jobName));
        event.put("inputs", List.of(input));
        event.put("outputs", List.of(output));
        return event;
    }

    /**
     * Build a RUNNING event for a sink connector (Kafka to sink system) with column-level lineage.
     *
     * @param jobName      the connector + task name (e.g., "inventory-jdbc-sink.0")
     * @param topic        the Kafka topic name
     * @param valueSchema  the value schema from the Connect record
     */
    public Map<String, Object> buildSinkRunningEvent(
            final String jobName,
            final String topic,
            final Schema valueSchema) {

        final List<Map<String, String>> fields = SchemaFacetExtractor.extractFields(valueSchema);
        final List<String> fieldNames = SchemaFacetExtractor.fieldNames(valueSchema);

        final String sinkNs = DatasetNameResolver.resolveSinkNamespace(config);
        final String sinkDataset = DatasetNameResolver.resolveSinkDatasetName(topic);
        final String kafkaNs = DatasetNameResolver.kafkaNamespace(config.kafkaBootstrapServers());

        // Input: Kafka topic
        final Map<String, Object> input = buildDataset(kafkaNs, topic, fields, null);

        // Output: sink dataset with column lineage
        final Map<String, Object> columnLineage =
                ColumnLineageFacetBuilder.buildForSink(fieldNames, kafkaNs, topic);
        final Map<String, Object> output = buildDataset(sinkNs, sinkDataset, fields, columnLineage);

        final Map<String, Object> event = new LinkedHashMap<>();
        event.put("eventType", "RUNNING");
        event.put("eventTime", ISO_FMT.format(Instant.now()));
        event.put("producer", PRODUCER);
        event.put("schemaURL", SCHEMA_URL);
        event.put("run", buildRun(topic));
        event.put("job", buildJob(jobName));
        event.put("inputs", List.of(input));
        event.put("outputs", List.of(output));
        return event;
    }

    private Map<String, Object> buildRun(final String topic) {
        final Map<String, Object> run = new LinkedHashMap<>();
        run.put("runId", runIdForTopic(topic));
        return run;
    }

    /**
     * Get all tracked topic runIds (for emitting COMPLETE events on close).
     */
    public Map<String, String> getRunIdsByTopic() {
        return Map.copyOf(runIdsByTopic);
    }

    private Map<String, Object> buildJob(final String jobName) {
        final Map<String, Object> job = new LinkedHashMap<>();
        job.put("namespace", config.namespace());
        job.put("name", jobName);
        return job;
    }

    private Map<String, Object> buildDataset(
            final String namespace,
            final String name,
            final List<Map<String, String>> fields,
            final Map<String, Object> columnLineageFacet) {

        final Map<String, Object> dataset = new LinkedHashMap<>();
        dataset.put("namespace", namespace);
        dataset.put("name", name);

        final Map<String, Object> facets = new LinkedHashMap<>();

        // Schema facet
        final Map<String, Object> schemaFacet = new LinkedHashMap<>();
        schemaFacet.put("_producer", PRODUCER);
        schemaFacet.put("_schemaURL",
                "https://openlineage.io/spec/facets/1-0-1/SchemaDatasetFacet.json");
        schemaFacet.put("fields", fields);
        facets.put("schema", schemaFacet);

        // Column lineage facet (only on output datasets)
        if (columnLineageFacet != null) {
            facets.put("columnLineage", columnLineageFacet);
        }

        dataset.put("facets", facets);
        return dataset;
    }

}
