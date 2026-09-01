package de.otto.anthology

import de.otto.anthology.config.AdditionalKafkaProperty
import de.otto.anthology.config.asMap
import de.otto.anthology.kafka.ClusterName
import de.otto.anthology.kafka.Consumer
import de.otto.anthology.kafka.ConsumerName
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.kafka.TopicName
import ox.flow.Flow
import ox.kafka.KafkaFlow
import pureconfig.ConfigReader

object KafkaSource:

    def apply(settings: KafkaSourceSettings): Flow[Passthrough] =
        KafkaFlow
            .subscribe(settings.consumer, settings.config.topic.toString)
            .map(rec => Passthrough(rec, settings.consumerName))

case class KafkaSourceConfig(
    cluster: ClusterName,
    topic: TopicName,
    consumerGroup: String,
    additionalConsumerProperties: Option[Seq[AdditionalKafkaProperty]]
) derives ConfigReader:
    def additionalConsumerPropertiesAsMap: Map[String, String] =
        additionalConsumerProperties.getOrElse(Seq.empty).asMap

case class KafkaSourceSettings(
    config: KafkaSourceConfig,
    consumerName: ConsumerName,
    consumer: Consumer
)
