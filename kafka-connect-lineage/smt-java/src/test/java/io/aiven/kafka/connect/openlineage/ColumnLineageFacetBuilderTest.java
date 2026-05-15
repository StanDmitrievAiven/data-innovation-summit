package io.aiven.kafka.connect.openlineage;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ColumnLineageFacetBuilderTest {

    @Test
    @SuppressWarnings("unchecked")
    void build_createsCorrectStructure() {
        final List<String> fieldNames = List.of("id", "name", "email");
        final String inputNs = "postgres://localhost:5432";
        final String inputDataset = "public.customers";

        final Map<String, Object> facet = ColumnLineageFacetBuilder.build(
                fieldNames, inputNs, inputDataset);

        assertNotNull(facet.get("_producer"));
        assertNotNull(facet.get("_schemaURL"));

        final Map<String, Object> fields = (Map<String, Object>) facet.get("fields");
        assertEquals(3, fields.size());
        assertTrue(fields.containsKey("id"));
        assertTrue(fields.containsKey("name"));
        assertTrue(fields.containsKey("email"));

        // Check a specific field entry
        final Map<String, Object> idEntry = (Map<String, Object>) fields.get("id");
        final List<Map<String, Object>> inputFields =
                (List<Map<String, Object>>) idEntry.get("inputFields");
        assertEquals(1, inputFields.size());

        final Map<String, Object> inputField = inputFields.get(0);
        assertEquals(inputNs, inputField.get("namespace"));
        assertEquals(inputDataset, inputField.get("name"));
        assertEquals("id", inputField.get("field"));

        final List<Map<String, String>> transformations =
                (List<Map<String, String>>) inputField.get("transformations");
        assertEquals(1, transformations.size());
        assertEquals("DIRECT", transformations.get(0).get("type"));
        assertEquals("IDENTITY", transformations.get(0).get("subtype"));
    }

    @Test
    void build_emptyFields_returnsEmptyFieldsMap() {
        final Map<String, Object> facet = ColumnLineageFacetBuilder.build(
                List.of(), "ns", "ds");

        @SuppressWarnings("unchecked")
        final Map<String, Object> fields = (Map<String, Object>) facet.get("fields");
        assertTrue(fields.isEmpty());
    }
}
