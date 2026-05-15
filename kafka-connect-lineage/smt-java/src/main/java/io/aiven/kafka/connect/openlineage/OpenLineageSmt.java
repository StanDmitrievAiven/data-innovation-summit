package io.aiven.kafka.connect.openlineage;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Kafka Connect Single Message Transform (SMT) that emits OpenLineage events
 * with column-level lineage for ANY connector.
 *
 * <p>This SMT is a <b>pass-through</b> — it never modifies records. It observes the record
 * schema and emits OpenLineage events as a side effect.</p>
 *
 * <p>Each topic gets its own OpenLineage job + run, so the lineage backend builds a separate
 * lineage graph per topic: {@code input dataset to job to output dataset}.</p>
 *
 * @param <R> the record type (SourceRecord or SinkRecord)
 */
public class OpenLineageSmt<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOG = LoggerFactory.getLogger(OpenLineageSmt.class);

    private OpenLineageSmtConfig config;
    private OpenLineageTransport transport;
    private OpenLineageEventBuilder eventBuilder;

    private boolean isSink;
    private final Set<String> emittedTopics = new HashSet<>();

    @Override
    public void configure(final Map<String, ?> props) {
        this.config = new OpenLineageSmtConfig(props);
        this.transport = new OpenLineageTransport(
                config.transportConfigPath(), config.transportType());
        this.eventBuilder = new OpenLineageEventBuilder(config);
        this.isSink = DatasetNameResolver.isSinkConnector(config.connectorClass());

        LOG.info("OpenLineage SMT configured: namespace={}, connector={}, isSink={}",
                config.namespace(), config.connectorClass(), isSink);
    }

    @Override
    public R apply(final R record) {
        if (record == null) {
            return null;
        }

        try {
            final String topic = record.topic();
            if (topic != null && !emittedTopics.contains(topic)) {
                final Schema valueSchema = record.valueSchema();
                if (valueSchema != null) {
                    final String jobName = resolveJobName(topic);

                    // Emit START to RUNNING to COMPLETE for this topic's job.
                    // The lineage backend needs a COMPLETE event with inputs/outputs
                    // to build lineage graph edges.
                    transport.emit(eventBuilder.buildLifecycleEvent("START", jobName, topic));
                    emitLifecycleWithIO("RUNNING", jobName, topic, valueSchema);
                    emitLifecycleWithIO("COMPLETE", jobName, topic, valueSchema);

                    emittedTopics.add(topic);
                    LOG.info("OpenLineage events emitted for job={} topic={} (fields: {})",
                            jobName, topic, SchemaFacetExtractor.fieldNames(valueSchema));
                }
            }
        } catch (final Exception e) {
            // Never fail the pipeline — log and continue
            LOG.warn("OpenLineage SMT error (non-fatal): {}", e.getMessage(), e);
        }

        // Always pass through the record unmodified
        return record;
    }

    private void emitLifecycleWithIO(
            final String eventType,
            final String jobName,
            final String topic,
            final Schema valueSchema) {

        final Map<String, Object> event;
        if (isSink) {
            event = eventBuilder.buildSinkRunningEvent(jobName, topic, valueSchema);
        } else {
            event = eventBuilder.buildSourceRunningEvent(jobName, topic, valueSchema);
        }
        // Override the eventType (RUNNING to COMPLETE for the final event)
        event.put("eventType", eventType);
        transport.emit(event);
    }

    /**
     * Build a per-topic job name. Each topic maps to its own OpenLineage job
     * so that lineage edges are 1:1 (one input to one job to one output).
     */
    private String resolveJobName(final String topic) {
        final String prefix = config.connectorClass().isEmpty()
                ? "connect"
                : simpleClassName(config.connectorClass());
        return prefix + "." + topic;
    }

    private static String simpleClassName(final String fqcn) {
        final int lastDot = fqcn.lastIndexOf('.');
        return lastDot >= 0 ? fqcn.substring(lastDot + 1) : fqcn;
    }

    @Override
    public ConfigDef config() {
        return OpenLineageSmtConfig.CONFIG_DEF;
    }

    @Override
    public void close() {
        // COMPLETE events are already sent per-topic in apply().
        // Nothing to do here.
    }
}
