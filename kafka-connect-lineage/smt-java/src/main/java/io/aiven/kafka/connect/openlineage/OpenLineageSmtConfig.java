package io.aiven.kafka.connect.openlineage;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Configuration for the OpenLineage SMT.
 *
 * <p>Users specify these as {@code transforms.openlineage.*} in connector config.</p>
 */
public class OpenLineageSmtConfig extends AbstractConfig {

    public static final String NAMESPACE = "namespace";
    public static final String NAMESPACE_DOC = "OpenLineage job namespace (e.g., 'kafka-connect-prod')";
    public static final String NAMESPACE_DEFAULT = "kafka-connect";

    public static final String TRANSPORT_CONFIG_PATH = "transport.config.path";
    public static final String TRANSPORT_CONFIG_PATH_DOC =
            "Path to openlineage.yml transport config file";
    public static final String TRANSPORT_CONFIG_PATH_DEFAULT = "/opt/openlineage/openlineage.yml";

    public static final String CONNECTOR_CLASS = "connector.class";
    public static final String CONNECTOR_CLASS_DOC =
            "The connector class (e.g., io.debezium.connector.postgresql.PostgresConnector). "
                    + "Used to resolve dataset namespace and name.";
    public static final String CONNECTOR_CLASS_DEFAULT = "";

    public static final String SOURCE_NAMESPACE = "source.namespace";
    public static final String SOURCE_NAMESPACE_DOC =
            "OpenLineage namespace for the source dataset (e.g., 'postgres://host:5432')";
    public static final String SOURCE_NAMESPACE_DEFAULT = "";

    public static final String SOURCE_DATASET_PREFIX = "source.dataset.prefix";
    public static final String SOURCE_DATASET_PREFIX_DOC =
            "Prefix for source dataset name (e.g., 'public' for public.table_name)";
    public static final String SOURCE_DATASET_PREFIX_DEFAULT = "";

    public static final String SINK_NAMESPACE = "sink.namespace";
    public static final String SINK_NAMESPACE_DOC =
            "OpenLineage namespace for the sink dataset (e.g., 'postgres://host:5432')";
    public static final String SINK_NAMESPACE_DEFAULT = "";

    public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
    public static final String KAFKA_BOOTSTRAP_SERVERS_DOC =
            "Kafka bootstrap servers for Kafka dataset namespace";
    public static final String KAFKA_BOOTSTRAP_SERVERS_DEFAULT = "kafka:9092";

    public static final String TRANSPORT_TYPE = "transport.type";
    public static final String TRANSPORT_TYPE_DOC =
            "Transport type: 'http' or 'console' (for debugging)";
    public static final String TRANSPORT_TYPE_DEFAULT = "http";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(NAMESPACE, ConfigDef.Type.STRING, NAMESPACE_DEFAULT,
                    ConfigDef.Importance.HIGH, NAMESPACE_DOC)
            .define(TRANSPORT_CONFIG_PATH, ConfigDef.Type.STRING, TRANSPORT_CONFIG_PATH_DEFAULT,
                    ConfigDef.Importance.HIGH, TRANSPORT_CONFIG_PATH_DOC)
            .define(CONNECTOR_CLASS, ConfigDef.Type.STRING, CONNECTOR_CLASS_DEFAULT,
                    ConfigDef.Importance.MEDIUM, CONNECTOR_CLASS_DOC)
            .define(SOURCE_NAMESPACE, ConfigDef.Type.STRING, SOURCE_NAMESPACE_DEFAULT,
                    ConfigDef.Importance.MEDIUM, SOURCE_NAMESPACE_DOC)
            .define(SOURCE_DATASET_PREFIX, ConfigDef.Type.STRING, SOURCE_DATASET_PREFIX_DEFAULT,
                    ConfigDef.Importance.LOW, SOURCE_DATASET_PREFIX_DOC)
            .define(SINK_NAMESPACE, ConfigDef.Type.STRING, SINK_NAMESPACE_DEFAULT,
                    ConfigDef.Importance.MEDIUM, SINK_NAMESPACE_DOC)
            .define(KAFKA_BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, KAFKA_BOOTSTRAP_SERVERS_DEFAULT,
                    ConfigDef.Importance.MEDIUM, KAFKA_BOOTSTRAP_SERVERS_DOC)
            .define(TRANSPORT_TYPE, ConfigDef.Type.STRING, TRANSPORT_TYPE_DEFAULT,
                    ConfigDef.Importance.LOW, TRANSPORT_TYPE_DOC);

    public OpenLineageSmtConfig(final Map<String, ?> props) {
        super(CONFIG_DEF, props);
    }

    public String namespace() {
        return getString(NAMESPACE);
    }

    public String transportConfigPath() {
        return getString(TRANSPORT_CONFIG_PATH);
    }

    public String connectorClass() {
        return getString(CONNECTOR_CLASS);
    }

    public String sourceNamespace() {
        return getString(SOURCE_NAMESPACE);
    }

    public String sourceDatasetPrefix() {
        return getString(SOURCE_DATASET_PREFIX);
    }

    public String sinkNamespace() {
        return getString(SINK_NAMESPACE);
    }

    public String kafkaBootstrapServers() {
        return getString(KAFKA_BOOTSTRAP_SERVERS);
    }

    public String transportType() {
        return getString(TRANSPORT_TYPE);
    }
}
