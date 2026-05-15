package io.aiven.kafka.connect.openlineage;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OpenLineageSmtTest {

    private final OpenLineageSmt<SourceRecord> sourceSmt = new OpenLineageSmt<>();
    private final OpenLineageSmt<SinkRecord> sinkSmt = new OpenLineageSmt<>();

    @AfterEach
    void tearDown() {
        sourceSmt.close();
        sinkSmt.close();
    }

    @Test
    void apply_sourceRecord_passesThrough() {
        sourceSmt.configure(makeSourceConfig());

        final Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct value = new Struct(valueSchema)
                .put("id", 1)
                .put("name", "Alice");

        final SourceRecord record = new SourceRecord(
                Map.of("server", "test"),
                Map.of("lsn", 1L),
                "inventory.public.customers",
                null,
                valueSchema,
                value);

        final SourceRecord result = sourceSmt.apply(record);

        assertSame(record, result);
        assertSame(value, result.value());
    }

    @Test
    void apply_sinkRecord_passesThrough() {
        sinkSmt.configure(makeSinkConfig());

        final Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Struct value = new Struct(valueSchema)
                .put("id", 1)
                .put("name", "Bob");

        final SinkRecord record = new SinkRecord(
                "inventory.public.customers",
                0, null, null,
                valueSchema, value,
                0L);

        assertSame(record, sinkSmt.apply(record));
    }

    @Test
    void apply_nullRecord_returnsNull() {
        sourceSmt.configure(makeSourceConfig());
        assertNull(sourceSmt.apply(null));
    }

    private static Map<String, String> makeSourceConfig() {
        final Map<String, String> props = new HashMap<>();
        props.put("namespace", "test-namespace");
        props.put("transport.type", "console");
        props.put("transport.config.path", "/dev/null");
        props.put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        props.put("source.namespace", "postgres://localhost:5432");
        props.put("source.dataset.prefix", "public");
        props.put("kafka.bootstrap.servers", "kafka:9092");
        return props;
    }

    private static Map<String, String> makeSinkConfig() {
        final Map<String, String> props = new HashMap<>();
        props.put("namespace", "test-namespace");
        props.put("transport.type", "console");
        props.put("transport.config.path", "/dev/null");
        props.put("connector.class", "io.aiven.connect.jdbc.JdbcSinkConnector");
        props.put("sink.namespace", "postgres://localhost:5432");
        props.put("kafka.bootstrap.servers", "kafka:9092");
        return props;
    }
}
