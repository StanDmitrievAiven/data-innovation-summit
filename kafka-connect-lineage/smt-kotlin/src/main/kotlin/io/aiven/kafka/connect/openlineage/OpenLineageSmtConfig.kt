package io.aiven.kafka.connect.openlineage

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.config.ConfigDef

/**
 * Configuration for the OpenLineage SMT.
 *
 * Users specify these as `transforms.openlineage.*` in connector config.
 *
 * @param props the raw configuration properties from the connector
 */
class OpenLineageSmtConfig(props: Map<String, *>) : AbstractConfig(CONFIG_DEF, props) {

    fun namespace(): String = getString(NAMESPACE)
    fun transportConfigPath(): String = getString(TRANSPORT_CONFIG_PATH)
    fun connectorClass(): String = getString(CONNECTOR_CLASS)
    fun sourceNamespace(): String = getString(SOURCE_NAMESPACE)
    fun sourceDatasetPrefix(): String = getString(SOURCE_DATASET_PREFIX)
    fun sourceDatasetDatabase(): String = getString(SOURCE_DATASET_DATABASE)
    fun sinkNamespace(): String = getString(SINK_NAMESPACE)
    fun sinkDatasetDatabase(): String = getString(SINK_DATASET_DATABASE)
    fun kafkaBootstrapServers(): String = getString(KAFKA_BOOTSTRAP_SERVERS)
    fun transportType(): String = getString(TRANSPORT_TYPE)

    companion object {
        const val NAMESPACE = "namespace"
        const val TRANSPORT_CONFIG_PATH = "transport.config.path"
        const val CONNECTOR_CLASS = "connector.class"
        const val SOURCE_NAMESPACE = "source.namespace"
        const val SOURCE_DATASET_PREFIX = "source.dataset.prefix"
        const val SOURCE_DATASET_DATABASE = "source.dataset.database"
        const val SINK_NAMESPACE = "sink.namespace"
        const val SINK_DATASET_DATABASE = "sink.dataset.database"
        const val KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers"
        const val TRANSPORT_TYPE = "transport.type"

        val CONFIG_DEF: ConfigDef = ConfigDef()
            .define(NAMESPACE, ConfigDef.Type.STRING, "kafka-connect",
                ConfigDef.Importance.HIGH, "OpenLineage job namespace (e.g., 'kafka-connect-prod')")
            .define(TRANSPORT_CONFIG_PATH, ConfigDef.Type.STRING, "/opt/openlineage/openlineage.yml",
                ConfigDef.Importance.HIGH, "Path to openlineage.yml transport config file")
            .define(CONNECTOR_CLASS, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.MEDIUM, "The connector class, used to resolve dataset namespace and name")
            .define(SOURCE_NAMESPACE, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.MEDIUM, "OpenLineage namespace for the source dataset")
            .define(SOURCE_DATASET_PREFIX, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.LOW, "Prefix for source dataset name (e.g., 'public')")
            .define(SOURCE_DATASET_DATABASE, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.LOW, "Database name to prepend to source dataset names (e.g., 'ecommerce')")
            .define(SINK_NAMESPACE, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.MEDIUM, "OpenLineage namespace for the sink dataset")
            .define(SINK_DATASET_DATABASE, ConfigDef.Type.STRING, "",
                ConfigDef.Importance.LOW, "Database name to prepend to sink dataset names (e.g., 'analytics')")
            .define(KAFKA_BOOTSTRAP_SERVERS, ConfigDef.Type.STRING, "kafka:9092",
                ConfigDef.Importance.MEDIUM, "Kafka bootstrap servers for Kafka dataset namespace")
            .define(TRANSPORT_TYPE, ConfigDef.Type.STRING, "http",
                ConfigDef.Importance.LOW, "Transport type: 'http' or 'console' (for debugging)")
    }
}
