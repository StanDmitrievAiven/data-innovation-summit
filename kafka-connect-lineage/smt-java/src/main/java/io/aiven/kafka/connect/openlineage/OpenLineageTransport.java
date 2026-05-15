package io.aiven.kafka.connect.openlineage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * Sends OpenLineage events to a configured endpoint (any OpenLineage-compatible backend, or console for debugging).
 *
 * <p>Reads transport configuration from an {@code openlineage.yml} file or falls back to
 * environment variables ({@code OPENLINEAGE_URL}).</p>
 *
 * <p>Supports optional token-based authentication via the {@code auth} section in the config:</p>
 * <pre>
 * transport:
 *   type: http
 *   url: https://datahub-gms.example.com
 *   endpoint: openapi/openlineage/api/v1/lineage
 *   auth:
 *     type: api_key
 *     api_key: your-token
 * </pre>
 */
public class OpenLineageTransport {

    private static final Logger LOG = LoggerFactory.getLogger(OpenLineageTransport.class);
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private final String transportType;
    private String url;
    private String endpoint;
    private String authToken;
    private HttpClient httpClient;

    public OpenLineageTransport(final String configPath, final String transportType) {
        this.transportType = transportType;
        if ("http".equals(transportType)) {
            loadHttpConfig(configPath);
            this.httpClient = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10))
                    .build();
        }
    }

    @SuppressWarnings("unchecked")
    private void loadHttpConfig(final String configPath) {
        // Try config file first
        if (configPath != null && !configPath.isEmpty()) {
            try {
                final File configFile = new File(configPath);
                if (configFile.exists()) {
                    final Map<String, Object> config = YAML_MAPPER.readValue(configFile, Map.class);
                    final Map<String, Object> transport = (Map<String, Object>) config.get("transport");
                    if (transport != null) {
                        this.url = (String) transport.get("url");
                        this.endpoint = (String) transport.getOrDefault("endpoint", "api/v1/lineage");

                        // Read auth config
                        final Map<String, Object> auth = (Map<String, Object>) transport.get("auth");
                        if (auth != null) {
                            this.authToken = (String) auth.get("api_key");
                            if (this.authToken != null) {
                                LOG.info("OpenLineage transport: {} -> {}/{} (with auth)", transportType, url, endpoint);
                            }
                        } else {
                            LOG.info("OpenLineage transport: {} -> {}/{} (no auth)", transportType, url, endpoint);
                        }
                        return;
                    }
                }
            } catch (final Exception e) {
                LOG.warn("Failed to read OpenLineage config from {}: {}", configPath, e.getMessage());
            }
        }

        // Fallback: environment variable
        this.url = System.getenv("OPENLINEAGE_URL");
        this.endpoint = "api/v1/lineage";
        if (this.url == null || this.url.isEmpty()) {
            this.url = "http://localhost:5000";
            LOG.warn("No OpenLineage URL configured, defaulting to {}", this.url);
        }
    }

    /**
     * Send an OpenLineage event.
     *
     * @param event the event as a Map (will be serialized to JSON)
     */
    public void emit(final Map<String, Object> event) {
        try {
            final String json = JSON_MAPPER.writeValueAsString(event);

            if ("console".equals(transportType)) {
                LOG.info("OpenLineage event:\n{}", JSON_MAPPER.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(event));
                return;
            }

            if (httpClient == null || url == null) {
                LOG.warn("OpenLineage HTTP transport not configured, skipping event");
                return;
            }

            final String fullUrl = url.endsWith("/")
                    ? url + endpoint
                    : url + "/" + endpoint;

            final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(fullUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .timeout(Duration.ofSeconds(10));

            if (authToken != null) {
                requestBuilder.header("Authorization", "Bearer " + authToken);
            }

            final HttpResponse<String> response = httpClient.send(requestBuilder.build(),
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                LOG.debug("OpenLineage event sent successfully (HTTP {})", response.statusCode());
            } else {
                LOG.warn("OpenLineage event failed (HTTP {}): {}", response.statusCode(),
                        response.body());
            }
        } catch (final Exception e) {
            LOG.warn("Failed to send OpenLineage event: {}", e.getMessage());
        }
    }
}
