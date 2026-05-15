package io.aiven.kafka.connect.openlineage;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OpenLineageEventBuilderTest {

    private OpenLineageEventBuilder builder;

    @BeforeEach
    void setUp() {
        final Map<String, String> props = new HashMap<>();
        props.put("namespace", "test-namespace");
        props.put("transport.config.path", "/dev/null");
        props.put("transport.type", "console");
        props.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        props.put("source.namespace", "postgres://localhost:5432");
        props.put("source.dataset.prefix", "public");
        props.put("kafka.bootstrap.servers", "kafka:9092");
        final OpenLineageSmtConfig config = new OpenLineageSmtConfig(props);
        builder = new OpenLineageEventBuilder(config);
    }

    @Test
    @SuppressWarnings("unchecked")
    void buildLifecycleEvent_start() {
        final Map<String, Object> event = builder.buildLifecycleEvent(
                "START", "test-connector", "test-topic");

        assertEquals("START", event.get("eventType"));
        assertNotNull(event.get("eventTime"));
        assertNotNull(event.get("producer"));
        assertNotNull(event.get("schemaURL"));

        final Map<String, Object> run = (Map<String, Object>) event.get("run");
        assertNotNull(run.get("runId"));

        final Map<String, Object> job = (Map<String, Object>) event.get("job");
        assertEquals("test-namespace", job.get("namespace"));
        assertEquals("test-connector", job.get("name"));

        final List<Object> inputs = (List<Object>) event.get("inputs");
        assertTrue(inputs.isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    void buildSourceRunningEvent_withSchema() {
        final Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("email", Schema.STRING_SCHEMA)
                .build();

        final Map<String, Object> event = builder.buildSourceRunningEvent(
                "inventory-source.0",
                "inventory.public.customers",
                valueSchema);

        assertEquals("RUNNING", event.get("eventType"));

        // Input: source dataset
        final List<Map<String, Object>> inputs = (List<Map<String, Object>>) event.get("inputs");
        assertEquals(1, inputs.size());
        assertEquals("postgres://localhost:5432", inputs.get(0).get("namespace"));
        assertEquals("public.customers", inputs.get(0).get("name"));

        final Map<String, Object> inputFacets = (Map<String, Object>) inputs.get(0).get("facets");
        final Map<String, Object> inputSchema = (Map<String, Object>) inputFacets.get("schema");
        final List<Map<String, String>> inputFields =
                (List<Map<String, String>>) inputSchema.get("fields");
        assertEquals(3, inputFields.size());
        assertEquals("id", inputFields.get(0).get("name"));

        // Output: Kafka topic
        final List<Map<String, Object>> outputs = (List<Map<String, Object>>) event.get("outputs");
        assertEquals(1, outputs.size());
        assertEquals("kafka://kafka:9092", outputs.get(0).get("namespace"));
        assertEquals("inventory.public.customers", outputs.get(0).get("name"));

        final Map<String, Object> outputFacets =
                (Map<String, Object>) outputs.get(0).get("facets");
        assertNotNull(outputFacets.get("schema"));
        assertNotNull(outputFacets.get("columnLineage"));

        final Map<String, Object> columnLineage =
                (Map<String, Object>) outputFacets.get("columnLineage");
        final Map<String, Object> clFields = (Map<String, Object>) columnLineage.get("fields");
        assertEquals(3, clFields.size());
        assertTrue(clFields.containsKey("id"));
        assertTrue(clFields.containsKey("name"));
        assertTrue(clFields.containsKey("email"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void buildSinkRunningEvent_withSchema() {
        final Map<String, String> sinkProps = new HashMap<>();
        sinkProps.put("namespace", "test-namespace");
        sinkProps.put("transport.config.path", "/dev/null");
        sinkProps.put("transport.type", "console");
        sinkProps.put("connector.class", "io.aiven.connect.jdbc.JdbcSinkConnector");
        sinkProps.put("sink.namespace", "postgres://target:5432");
        sinkProps.put("kafka.bootstrap.servers", "kafka:9092");
        final OpenLineageSmtConfig sinkConfig = new OpenLineageSmtConfig(sinkProps);
        final OpenLineageEventBuilder sinkBuilder = new OpenLineageEventBuilder(sinkConfig);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();

        final Map<String, Object> event = sinkBuilder.buildSinkRunningEvent(
                "jdbc-sink.0",
                "inventory.public.customers",
                valueSchema);

        assertEquals("RUNNING", event.get("eventType"));

        final List<Map<String, Object>> inputs = (List<Map<String, Object>>) event.get("inputs");
        assertEquals(1, inputs.size());
        assertEquals("kafka://kafka:9092", inputs.get(0).get("namespace"));
        assertEquals("inventory.public.customers", inputs.get(0).get("name"));

        final List<Map<String, Object>> outputs = (List<Map<String, Object>>) event.get("outputs");
        assertEquals(1, outputs.size());
        assertEquals("postgres://target:5432", outputs.get(0).get("namespace"));
        assertEquals("public.customers", outputs.get(0).get("name"));

        final Map<String, Object> outputFacets =
                (Map<String, Object>) outputs.get(0).get("facets");
        assertNotNull(outputFacets.get("columnLineage"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void runId_isConsistentPerTopic() {
        final Map<String, Object> event1 = builder.buildLifecycleEvent("START", "test", "topic-a");
        final Map<String, Object> event2 = builder.buildLifecycleEvent("COMPLETE", "test", "topic-a");

        final Map<String, Object> run1 = (Map<String, Object>) event1.get("run");
        final Map<String, Object> run2 = (Map<String, Object>) event2.get("run");

        assertEquals(run1.get("runId"), run2.get("runId"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void runId_differsBetweenTopics() {
        final Map<String, Object> eventA = builder.buildLifecycleEvent("START", "job-a", "topic-a");
        final Map<String, Object> eventB = builder.buildLifecycleEvent("START", "job-b", "topic-b");

        final Map<String, Object> runA = (Map<String, Object>) eventA.get("run");
        final Map<String, Object> runB = (Map<String, Object>) eventB.get("run");

        assertNotEquals(runA.get("runId"), runB.get("runId"));
    }
}
