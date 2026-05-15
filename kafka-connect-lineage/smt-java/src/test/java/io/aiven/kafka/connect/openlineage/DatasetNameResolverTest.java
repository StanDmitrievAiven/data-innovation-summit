package io.aiven.kafka.connect.openlineage;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DatasetNameResolverTest {

    @Test
    void kafkaNamespace() {
        assertEquals("kafka://kafka:9092", DatasetNameResolver.kafkaNamespace("kafka:9092"));
        assertEquals("kafka://broker1:9092,broker2:9092",
                DatasetNameResolver.kafkaNamespace("broker1:9092,broker2:9092"));
    }

    @Test
    void resolveSourceNamespace_explicit() {
        final OpenLineageSmtConfig config = makeConfig(Map.of(
                "source.namespace", "postgres://myhost:5432"
        ));
        assertEquals("postgres://myhost:5432", DatasetNameResolver.resolveSourceNamespace(config));
    }

    @Test
    void resolveSourceNamespace_fallback() {
        final OpenLineageSmtConfig config = makeConfig(Map.of());
        assertEquals("kafka-connect", DatasetNameResolver.resolveSourceNamespace(config));
    }

    @Test
    void resolveSinkNamespace_explicit() {
        final OpenLineageSmtConfig config = makeConfig(Map.of(
                "sink.namespace", "postgres://target:5432"
        ));
        assertEquals("postgres://target:5432", DatasetNameResolver.resolveSinkNamespace(config));
    }

    @Test
    void resolveSourceDatasetName_debezium() {
        final OpenLineageSmtConfig config = makeConfig(Map.of(
                "connector.class", "io.debezium.connector.postgresql.PostgresConnector"
        ));

        // Debezium topic: prefix.schema.table to schema.table
        assertEquals("public.customers",
                DatasetNameResolver.resolveSourceDatasetName(config, "inventory.public.customers"));
        assertEquals("public.orders",
                DatasetNameResolver.resolveSourceDatasetName(config, "inventory.public.orders"));
    }

    @Test
    void resolveSourceDatasetName_debezium_twoPartTopic() {
        final OpenLineageSmtConfig config = makeConfig(Map.of(
                "connector.class", "io.debezium.connector.postgresql.PostgresConnector"
        ));

        // Two-part topic: schema.table to schema.table
        assertEquals("public.customers",
                DatasetNameResolver.resolveSourceDatasetName(config, "public.customers"));
    }

    @Test
    void resolveSourceDatasetName_withPrefix() {
        final OpenLineageSmtConfig config = makeConfig(Map.of(
                "source.dataset.prefix", "public"
        ));

        assertEquals("public.customers",
                DatasetNameResolver.resolveSourceDatasetName(config, "customers"));
    }

    @Test
    void resolveSinkDatasetName_threePartTopic() {
        assertEquals("public.customers",
                DatasetNameResolver.resolveSinkDatasetName("inventory.public.customers"));
    }

    @Test
    void resolveSinkDatasetName_twoPartTopic() {
        assertEquals("public.customers",
                DatasetNameResolver.resolveSinkDatasetName("public.customers"));
    }

    @Test
    void resolveSinkDatasetName_singlePartTopic() {
        assertEquals("customers", DatasetNameResolver.resolveSinkDatasetName("customers"));
    }

    @Test
    void isDebezium_true() {
        assertTrue(DatasetNameResolver.isDebezium(
                "io.debezium.connector.postgresql.PostgresConnector"));
        assertTrue(DatasetNameResolver.isDebezium(
                "io.debezium.connector.mysql.MySqlConnector"));
    }

    @Test
    void isDebezium_false() {
        assertFalse(DatasetNameResolver.isDebezium(
                "io.aiven.connect.jdbc.JdbcSinkConnector"));
        assertFalse(DatasetNameResolver.isDebezium(null));
        assertFalse(DatasetNameResolver.isDebezium(""));
    }

    @Test
    void isSinkConnector_true() {
        assertTrue(DatasetNameResolver.isSinkConnector(
                "io.aiven.connect.jdbc.JdbcSinkConnector"));
        assertTrue(DatasetNameResolver.isSinkConnector(
                "io.aiven.kafka.connect.s3.AivenKafkaConnectS3SinkConnector"));
    }

    @Test
    void isSinkConnector_false() {
        assertFalse(DatasetNameResolver.isSinkConnector(
                "io.debezium.connector.postgresql.PostgresConnector"));
        assertFalse(DatasetNameResolver.isSinkConnector(null));
    }

    @Test
    void extractDebeziumTableName() {
        assertEquals("public.customers",
                DatasetNameResolver.extractDebeziumTableName("inventory.public.customers"));
        assertEquals("public.orders",
                DatasetNameResolver.extractDebeziumTableName("dbserver1.public.orders"));
        assertEquals("schema.table",
                DatasetNameResolver.extractDebeziumTableName("a.b.c.schema.table"));
    }

    private static OpenLineageSmtConfig makeConfig(final Map<String, String> overrides) {
        final Map<String, String> props = new HashMap<>();
        // defaults
        props.put("namespace", "kafka-connect");
        props.put("transport.config.path", "/dev/null");
        props.putAll(overrides);
        return new OpenLineageSmtConfig(props);
    }
}
