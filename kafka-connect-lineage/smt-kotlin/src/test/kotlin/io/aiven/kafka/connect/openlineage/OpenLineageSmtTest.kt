package io.aiven.kafka.connect.openlineage

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.source.SourceRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test

class OpenLineageSmtTest {

    private val sourceSmt = OpenLineageSmt<SourceRecord>()
    private val sinkSmt = OpenLineageSmt<SinkRecord>()

    @AfterEach
    fun tearDown() {
        sourceSmt.close()
        sinkSmt.close()
    }

    @Test
    fun `apply source record passes through unmodified`() {
        sourceSmt.configure(makeSourceConfig())

        val valueSchema = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build()
        val value = Struct(valueSchema).put("id", 1).put("name", "Alice")

        val record = SourceRecord(
            mapOf("server" to "test"),
            mapOf("lsn" to 1L),
            "inventory.public.customers",
            null,
            valueSchema,
            value,
        )

        val result = sourceSmt.apply(record)

        assertSame(record, result)
        assertSame(value, result?.value())
    }

    @Test
    fun `apply sink record passes through unmodified`() {
        sinkSmt.configure(makeSinkConfig())

        val valueSchema = SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .build()
        val value = Struct(valueSchema).put("id", 1).put("name", "Bob")

        val record = SinkRecord(
            "inventory.public.customers", 0,
            null, null,
            valueSchema, value,
            0L,
        )

        assertSame(record, sinkSmt.apply(record))
    }

    @Test
    fun `apply null record returns null`() {
        sourceSmt.configure(makeSourceConfig())
        assertNull(sourceSmt.apply(null))
    }


    private fun makeSourceConfig(): MutableMap<String, String> = mutableMapOf(
        "namespace" to "test-namespace",
        "transport.type" to "console",
        "transport.config.path" to "/dev/null",
        "connector.class" to "io.debezium.connector.postgresql.PostgresConnector",
        "source.namespace" to "postgres://localhost:5432",
        "source.dataset.prefix" to "public",
        "kafka.bootstrap.servers" to "kafka:9092",
    )

    private fun makeSinkConfig(): MutableMap<String, String> = mutableMapOf(
        "namespace" to "test-namespace",
        "transport.type" to "console",
        "transport.config.path" to "/dev/null",
        "connector.class" to "io.aiven.connect.jdbc.JdbcSinkConnector",
        "sink.namespace" to "postgres://localhost:5432",
        "kafka.bootstrap.servers" to "kafka:9092",
    )
}
