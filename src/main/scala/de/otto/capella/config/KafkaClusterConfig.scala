package de.otto.capella.config

import de.otto.capella.kafka.ClusterName
import pureconfig.ConfigReader

case class KafkaClusterConfig(
    name: ClusterName,
    bootstrapServers: String
) derives ConfigReader

case class KafkaClusterSettings(
    config: KafkaClusterConfig,
    additionalProperties: Map[String, String]
)
