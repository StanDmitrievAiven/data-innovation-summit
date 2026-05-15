package io.aiven.kafka.connect.openlineage

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class OpenLineageEventBuilderTest {

    private lateinit var builder: OpenLineageEventBuilder

    @BeforeEach
    fun setUp() {
        val config = OpenLineageSmtConfig(
            mapOf(
                "namespace" to "test-namespace",
                "transport.config.path" to "/dev/null",
                "transport.type" to "console",
                "connector.class" to "io.debezium.connector.postgresql.PostgresConnector",
                "source.namespace" to "postgres://localhost:5432",
                "source.dataset.prefix" to "public",
                "kafka.bootstrap.servers" to "kafka:9092",
            )
        )
        builder = OpenLineageEventBuilder(config)
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `buildLifecycleEvent START`() {
        val event = builder.buildLifecycleEvent("START", "test-connector", "test-topic")

        assertEquals("START", event["eventType"])
        assertNotNull(event["eventTime"])
        assertNotNull(event["producer"])
        assertNotNull(event["schemaURL"])

        val run = event["run"] as Map<String, Any>
        assertNotNull(run["runId"])

        val job = event["job"] as Map<String, Any>
        assertEquals("test-namespace", job["namespace"])
        assertEquals("test-connector", job["name"])

        val inputs = event["inputs"] as List<Any>
        assertTrue(inputs.isEmpty())
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `buildSourceRunningEvent with schema`() {
        val valueSchema = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("email", Schema.STRING_SCHEMA)
            .build()

        val event = builder.buildSourceRunningEvent(
            "inventory-source.0", "inventory.public.customers", valueSchema,
        )

        assertEquals("RUNNING", event["eventType"])

        // Input: source dataset
        val inputs = event["inputs"] as List<Map<String, Any>>
        assertEquals(1, inputs.size)
        assertEquals("postgres://localhost:5432", inputs[0]["namespace"])
        assertEquals("public.customers", inputs[0]["name"])

        val inputFacets = inputs[0]["facets"] as Map<String, Any>
        val inputSchema = inputFacets["schema"] as Map<String, Any>
        val inputFields = inputSchema["fields"] as List<Map<String, String>>
        assertEquals(3, inputFields.size)
        assertEquals("id", inputFields[0]["name"])

        // Output: Kafka topic
        val outputs = event["outputs"] as List<Map<String, Any>>
        assertEquals(1, outputs.size)
        assertEquals("kafka://kafka:9092", outputs[0]["namespace"])
        assertEquals("inventory.public.customers", outputs[0]["name"])

        val outputFacets = outputs[0]["facets"] as Map<String, Any>
        assertNotNull(outputFacets["schema"])
        assertNotNull(outputFacets["columnLineage"])

        val columnLineage = outputFacets["columnLineage"] as Map<String, Any>
        val clFields = columnLineage["fields"] as Map<String, Any>
        assertEquals(3, clFields.size)
        assertTrue(clFields.containsKey("id"))
        assertTrue(clFields.containsKey("name"))
        assertTrue(clFields.containsKey("email"))
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `buildSinkRunningEvent with schema`() {
        val sinkConfig = OpenLineageSmtConfig(
            mapOf(
                "namespace" to "test-namespace",
                "transport.config.path" to "/dev/null",
                "transport.type" to "console",
                "connector.class" to "io.aiven.connect.jdbc.JdbcSinkConnector",
                "sink.namespace" to "postgres://target:5432",
                "kafka.bootstrap.servers" to "kafka:9092",
            )
        )
        val sinkBuilder = OpenLineageEventBuilder(sinkConfig)

        val valueSchema = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build()

        val event = sinkBuilder.buildSinkRunningEvent(
            "jdbc-sink.0", "inventory.public.customers", valueSchema,
        )

        assertEquals("RUNNING", event["eventType"])

        val inputs = event["inputs"] as List<Map<String, Any>>
        assertEquals(1, inputs.size)
        assertEquals("kafka://kafka:9092", inputs[0]["namespace"])
        assertEquals("inventory.public.customers", inputs[0]["name"])

        val outputs = event["outputs"] as List<Map<String, Any>>
        assertEquals(1, outputs.size)
        assertEquals("postgres://target:5432", outputs[0]["namespace"])
        assertEquals("public.customers", outputs[0]["name"])

        val outputFacets = outputs[0]["facets"] as Map<String, Any>
        assertNotNull(outputFacets["columnLineage"])
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `runId is consistent per topic`() {
        val event1 = builder.buildLifecycleEvent("START", "test", "topic-a")
        val event2 = builder.buildLifecycleEvent("COMPLETE", "test", "topic-a")

        val run1 = event1["run"] as Map<String, Any>
        val run2 = event2["run"] as Map<String, Any>
        assertEquals(run1["runId"], run2["runId"])
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `runId differs between topics`() {
        val eventA = builder.buildLifecycleEvent("START", "job-a", "topic-a")
        val eventB = builder.buildLifecycleEvent("START", "job-b", "topic-b")

        val runA = eventA["run"] as Map<String, Any>
        val runB = eventB["run"] as Map<String, Any>
        assertNotEquals(runA["runId"], runB["runId"])
    }

}
