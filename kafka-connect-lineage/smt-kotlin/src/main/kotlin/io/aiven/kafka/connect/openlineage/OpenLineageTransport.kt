package io.aiven.kafka.connect.openlineage

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.slf4j.LoggerFactory
import java.io.File
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * Sends OpenLineage events to a configured endpoint (any OpenLineage-compatible backend, or console for debugging).
 *
 * Reads transport configuration from an `openlineage.yml` file or falls back to
 * the `OPENLINEAGE_URL` environment variable.
 *
 * Supports optional token-based authentication via the `auth` section in the config:
 * ```yaml
 * transport:
 *   type: http
 *   url: https://datahub-gms.example.com
 *   endpoint: openapi/openlineage/api/v1/lineage
 *   auth:
 *     type: api_key
 *     api_key: <your-token>
 * ```
 *
 * @param configPath path to the `openlineage.yml` config file
 * @param transportType the transport type (`http` or `console`)
 */
class OpenLineageTransport(configPath: String, private val transportType: String) {

    private var url: String? = null
    private var endpoint: String = DEFAULT_ENDPOINT
    private var authToken: String? = null
    private var httpClient: HttpClient? = null

    init {
        if (transportType == "http") {
            loadHttpConfig(configPath)
            httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build()
        }
    }

    /**
     * Sends an OpenLineage event.
     *
     * @param event the event as a map (will be serialized to JSON)
     */
    fun emit(event: Map<String, Any>) {
        runCatching {
            val json = JSON_MAPPER.writeValueAsString(event)

            if (transportType == "console") {
                log.info("OpenLineage event:\n{}", JSON_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(event))
                return
            }

            val client = httpClient
            val baseUrl = url
            if (client == null || baseUrl == null) {
                log.warn("OpenLineage HTTP transport not configured, skipping event")
                return
            }

            val fullUrl = if (baseUrl.endsWith("/")) "$baseUrl$endpoint" else "$baseUrl/$endpoint"

            val requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(fullUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .timeout(Duration.ofSeconds(10))

            if (authToken != null) {
                requestBuilder.header("Authorization", "Bearer $authToken")
            }

            val response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString())

            if (response.statusCode() in 200..299) {
                log.debug("OpenLineage event sent successfully (HTTP {})", response.statusCode())
            } else {
                log.warn("OpenLineage event failed (HTTP {}): {}", response.statusCode(), response.body())
            }
        }.onFailure { e ->
            log.warn("Failed to send OpenLineage event: {}", e.message)
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun loadHttpConfig(configPath: String) {
        if (configPath.isNotEmpty()) {
            runCatching {
                val configFile = File(configPath)
                if (configFile.exists()) {
                    val config = YAML_MAPPER.readValue(configFile, Map::class.java) as Map<String, Any>
                    val transport = config["transport"] as? Map<String, Any>
                    if (transport != null) {
                        url = transport["url"] as? String
                        endpoint = (transport["endpoint"] as? String) ?: DEFAULT_ENDPOINT

                        // Read auth config
                        val auth = transport["auth"] as? Map<String, Any>
                        if (auth != null) {
                            authToken = auth["api_key"] as? String
                            if (authToken != null) {
                                log.info("OpenLineage transport: {} -> {}/{} (with auth)", transportType, url, endpoint)
                            }
                        } else {
                            log.info("OpenLineage transport: {} -> {}/{} (no auth)", transportType, url, endpoint)
                        }
                        return
                    }
                }
            }.onFailure { e ->
                log.warn("Failed to read OpenLineage config from {}: {}", configPath, e.message)
            }
        }

        // Fallback: environment variable
        url = System.getenv("OPENLINEAGE_URL")?.ifEmpty { null } ?: DEFAULT_URL.also {
            log.warn("No OpenLineage URL configured, defaulting to {}", it)
        }
    }

    companion object {
        private val log = LoggerFactory.getLogger(OpenLineageTransport::class.java)
        private val JSON_MAPPER = ObjectMapper()
        private val YAML_MAPPER = ObjectMapper(YAMLFactory())
        private const val DEFAULT_ENDPOINT = "api/v1/lineage"
        private const val DEFAULT_URL = "http://localhost:5000"
    }
}
