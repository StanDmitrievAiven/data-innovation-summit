package io.aiven.kafka.connect.openlineage

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class DatasetNameResolverTest {

    @Test
    fun `kafkaNamespace formats correctly`() {
        assertEquals("kafka://kafka:9092", DatasetNameResolver.kafkaNamespace("kafka:9092"))
        assertEquals("kafka://broker1:9092,broker2:9092", DatasetNameResolver.kafkaNamespace("broker1:9092,broker2:9092"))
    }

    @Test
    fun `resolveSourceNamespace uses explicit value`() {
        val config = makeConfig(mapOf("source.namespace" to "postgres://myhost:5432"))
        assertEquals("postgres://myhost:5432", DatasetNameResolver.resolveSourceNamespace(config))
    }

    @Test
    fun `resolveSourceNamespace falls back to namespace`() {
        val config = makeConfig(emptyMap())
        assertEquals("kafka-connect", DatasetNameResolver.resolveSourceNamespace(config))
    }

    @Test
    fun `resolveSinkNamespace uses explicit value`() {
        val config = makeConfig(mapOf("sink.namespace" to "postgres://target:5432"))
        assertEquals("postgres://target:5432", DatasetNameResolver.resolveSinkNamespace(config))
    }

    @Test
    fun `resolveSourceDatasetName for Debezium three-part topic`() {
        val config = makeConfig(mapOf("connector.class" to "io.debezium.connector.postgresql.PostgresConnector"))
        assertEquals("public.customers", DatasetNameResolver.resolveSourceDatasetName(config, "inventory.public.customers"))
        assertEquals("public.orders", DatasetNameResolver.resolveSourceDatasetName(config, "inventory.public.orders"))
    }

    @Test
    fun `resolveSourceDatasetName for Debezium with database qualifier`() {
        val config = makeConfig(mapOf(
            "connector.class" to "io.debezium.connector.postgresql.PostgresConnector",
            "source.dataset.database" to "ecommerce",
        ))
        assertEquals("ecommerce.public.customers", DatasetNameResolver.resolveSourceDatasetName(config, "inventory.public.customers"))
        assertEquals("ecommerce.public.orders", DatasetNameResolver.resolveSourceDatasetName(config, "inventory.public.orders"))
    }

    @Test
    fun `resolveSourceDatasetName for Debezium two-part topic`() {
        val config = makeConfig(mapOf("connector.class" to "io.debezium.connector.postgresql.PostgresConnector"))
        assertEquals("public.customers", DatasetNameResolver.resolveSourceDatasetName(config, "public.customers"))
    }

    @Test
    fun `resolveSourceDatasetName with prefix`() {
        val config = makeConfig(mapOf("source.dataset.prefix" to "public"))
        assertEquals("public.customers", DatasetNameResolver.resolveSourceDatasetName(config, "customers"))
    }

    @Test
    fun `resolveSinkDatasetName for three-part topic`() {
        val config = makeConfig(emptyMap())
        assertEquals("public.customers", DatasetNameResolver.resolveSinkDatasetName(config, "inventory.public.customers"))
    }

    @Test
    fun `resolveSinkDatasetName with database qualifier`() {
        val config = makeConfig(mapOf("sink.dataset.database" to "analytics"))
        assertEquals("analytics.public.customers", DatasetNameResolver.resolveSinkDatasetName(config, "inventory.public.customers"))
    }

    @Test
    fun `resolveSinkDatasetName for two-part topic`() {
        val config = makeConfig(emptyMap())
        assertEquals("public.customers", DatasetNameResolver.resolveSinkDatasetName(config, "public.customers"))
    }

    @Test
    fun `resolveSinkDatasetName for single-part topic`() {
        val config = makeConfig(emptyMap())
        assertEquals("customers", DatasetNameResolver.resolveSinkDatasetName(config, "customers"))
    }

    @Test
    fun `isDebezium returns true for Debezium connectors`() {
        assertTrue(DatasetNameResolver.isDebezium("io.debezium.connector.postgresql.PostgresConnector"))
        assertTrue(DatasetNameResolver.isDebezium("io.debezium.connector.mysql.MySqlConnector"))
    }

    @Test
    fun `isDebezium returns false for non-Debezium connectors`() {
        assertFalse(DatasetNameResolver.isDebezium("io.aiven.connect.jdbc.JdbcSinkConnector"))
        assertFalse(DatasetNameResolver.isDebezium(""))
    }

    @Test
    fun `isSinkConnector returns true for sink connectors`() {
        assertTrue(DatasetNameResolver.isSinkConnector("io.aiven.connect.jdbc.JdbcSinkConnector"))
        assertTrue(DatasetNameResolver.isSinkConnector("io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector"))
    }

    @Test
    fun `isSinkConnector returns false for source connectors`() {
        assertFalse(DatasetNameResolver.isSinkConnector("io.debezium.connector.postgresql.PostgresConnector"))
    }

    @Test
    fun `extractDebeziumTableName extracts last two segments`() {
        assertEquals("public.customers", DatasetNameResolver.extractDebeziumTableName("inventory.public.customers"))
        assertEquals("public.orders", DatasetNameResolver.extractDebeziumTableName("dbserver1.public.orders"))
        assertEquals("schema.table", DatasetNameResolver.extractDebeziumTableName("a.b.c.schema.table"))
    }

    private fun makeConfig(overrides: Map<String, String>): OpenLineageSmtConfig {
        val props = mutableMapOf(
            "namespace" to "kafka-connect",
            "transport.config.path" to "/dev/null",
        )
        props.putAll(overrides)
        return OpenLineageSmtConfig(props)
    }
}
