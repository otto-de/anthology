package de.otto.anthology

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
    consumerGroup: String
) derives ConfigReader

case class KafkaSourceSettings(
    config: KafkaSourceConfig,
    consumerName: ConsumerName,
    consumer: Consumer
)
