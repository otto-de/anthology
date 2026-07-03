package de.otto.anthology.config

import de.otto.anthology.kafka.ClusterName
import pureconfig.ConfigReader

case class KafkaClusterConfig(
    name: ClusterName,
    bootstrapServers: String
) derives ConfigReader

case class KafkaClusterSettings(
    config: KafkaClusterConfig,
    additionalProperties: Map[String, String]
)
