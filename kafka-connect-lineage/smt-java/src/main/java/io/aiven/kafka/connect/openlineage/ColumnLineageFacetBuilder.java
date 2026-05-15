package io.aiven.kafka.connect.openlineage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the {@code ColumnLineageDatasetFacet} for an OpenLineage output dataset.
 *
 * <p>For each output field, records which input field(s) it came from, with a
 * DIRECT/IDENTITY transformation type (since Kafka Connect connectors move data as-is).</p>
 */
public final class ColumnLineageFacetBuilder {

    private ColumnLineageFacetBuilder() {
    }

    /**
     * Build column lineage facet mapping input fields to output fields.
     *
     * <p>Since connectors pass data through without transformation, each output column maps 1:1
     * to the corresponding input column with IDENTITY transformation.</p>
     *
     * @param fieldNames       list of column/field names
     * @param inputNamespace   OpenLineage namespace of the input dataset
     * @param inputDatasetName OpenLineage name of the input dataset
     * @return column lineage facet as a map structure
     */
    public static Map<String, Object> build(
            final List<String> fieldNames,
            final String inputNamespace,
            final String inputDatasetName) {

        final Map<String, Object> facet = new LinkedHashMap<>();
        facet.put("_producer", "https://github.com/OpenLineage/OpenLineage/tree/1.24.2/integration/kafka");
        facet.put("_schemaURL", "https://openlineage.io/spec/facets/1-0-1/ColumnLineageDatasetFacet.json");

        final Map<String, Object> fields = new LinkedHashMap<>();

        for (final String fieldName : fieldNames) {
            final Map<String, Object> inputField = new LinkedHashMap<>();
            inputField.put("namespace", inputNamespace);
            inputField.put("name", inputDatasetName);
            inputField.put("field", fieldName);

            final Map<String, String> transformation = new LinkedHashMap<>();
            transformation.put("type", "DIRECT");
            transformation.put("subtype", "IDENTITY");
            transformation.put("description", "Column passed through without transformation");

            inputField.put("transformations", List.of(transformation));

            final Map<String, Object> fieldEntry = new LinkedHashMap<>();
            fieldEntry.put("inputFields", List.of(inputField));

            fields.put(fieldName, fieldEntry);
        }

        facet.put("fields", fields);
        return facet;
    }

    /**
     * Build a column lineage facet for source connectors.
     *
     * <p>Input = source system (database, S3 bucket, HTTP endpoint, another Kafka topic, etc.),
     * Output = Kafka topic</p>
     */
    public static Map<String, Object> buildForSource(
            final List<String> fieldNames,
            final String sourceNamespace,
            final String sourceDatasetName) {
        return build(fieldNames, sourceNamespace, sourceDatasetName);
    }

    /**
     * Build a column lineage facet for sink connectors.
     *
     * <p>Input = Kafka topic, Output = sink system (database, S3 bucket, etc.)</p>
     */
    public static Map<String, Object> buildForSink(
            final List<String> fieldNames,
            final String inputNamespace,
            final String inputDatasetName) {
        return build(fieldNames, inputNamespace, inputDatasetName);
    }
}
