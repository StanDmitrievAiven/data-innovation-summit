package io.aiven.kafka.connect.openlineage

/**
 * Builds the [ColumnLineageDatasetFacet](https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json)
 * for an OpenLineage output dataset.
 *
 * For each output field, records which input field(s) it came from, with a
 * DIRECT/IDENTITY transformation type (since Kafka Connect connectors move data as-is).
 */
object ColumnLineageFacetBuilder {

    private const val PRODUCER = "https://github.com/OpenLineage/OpenLineage/tree/1.24.2/integration/kafka"
    private const val SCHEMA_URL = "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json"

    /**
     * Builds column lineage facet mapping input fields to output fields.
     *
     * Since connectors pass data through without transformation, each output column maps 1:1
     * to the corresponding input column with IDENTITY transformation.
     *
     * @param fieldNames list of column/field names
     * @param inputNamespace OpenLineage namespace of the input dataset
     * @param inputDatasetName OpenLineage name of the input dataset
     * @return column lineage facet as a map structure
     */
    fun build(
        fieldNames: List<String>,
        inputNamespace: String,
        inputDatasetName: String,
    ): Map<String, Any> {
        val fields = fieldNames.associateWith { fieldName ->
            mapOf(
                "inputFields" to listOf(
                    mapOf(
                        "namespace" to inputNamespace,
                        "name" to inputDatasetName,
                        "field" to fieldName,
                        "transformations" to listOf(
                            mapOf(
                                "type" to "DIRECT",
                                "subtype" to "IDENTITY",
                                "description" to "Column passed through without transformation",
                            )
                        ),
                    )
                )
            )
        }

        return linkedMapOf(
            "_producer" to PRODUCER,
            "_schemaURL" to SCHEMA_URL,
            "fields" to fields,
        )
    }

    /**
     * Builds a column lineage facet for source connectors.
     *
     * Input = the source system
     * Output = Kafka topic
     */
    fun buildForSource(
        fieldNames: List<String>,
        sourceNamespace: String,
        sourceDatasetName: String,
    ): Map<String, Any> = build(fieldNames, sourceNamespace, sourceDatasetName)

    /**
     * Builds a column lineage facet for sink connectors.
     *
     * Input = Kafka topic
     * Output = the sink system
     */
    fun buildForSink(
        fieldNames: List<String>,
        inputNamespace: String,
        inputDatasetName: String,
    ): Map<String, Any> = build(fieldNames, inputNamespace, inputDatasetName)
}
