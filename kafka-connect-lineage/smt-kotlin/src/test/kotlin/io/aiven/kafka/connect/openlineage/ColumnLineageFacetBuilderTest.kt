package io.aiven.kafka.connect.openlineage

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ColumnLineageFacetBuilderTest {

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `build creates correct structure`() {
        val facet = ColumnLineageFacetBuilder.build(
            fieldNames = listOf("id", "name", "email"),
            inputNamespace = "postgres://localhost:5432",
            inputDatasetName = "public.customers",
        )

        assertNotNull(facet["_producer"])
        assertNotNull(facet["_schemaURL"])

        val fields = facet["fields"] as Map<String, Any>
        assertEquals(3, fields.size)
        assertTrue(fields.containsKey("id"))
        assertTrue(fields.containsKey("name"))
        assertTrue(fields.containsKey("email"))

        val idEntry = fields["id"] as Map<String, Any>
        val inputFields = idEntry["inputFields"] as List<Map<String, Any>>
        assertEquals(1, inputFields.size)

        val inputField = inputFields[0]
        assertEquals("postgres://localhost:5432", inputField["namespace"])
        assertEquals("public.customers", inputField["name"])
        assertEquals("id", inputField["field"])

        val transformations = inputField["transformations"] as List<Map<String, String>>
        assertEquals(1, transformations.size)
        assertEquals("DIRECT", transformations[0]["type"])
        assertEquals("IDENTITY", transformations[0]["subtype"])
    }

    @Test
    @Suppress("UNCHECKED_CAST")
    fun `build with empty fields returns empty fields map`() {
        val facet = ColumnLineageFacetBuilder.build(emptyList(), "ns", "ds")
        val fields = facet["fields"] as Map<String, Any>
        assertTrue(fields.isEmpty())
    }

}
