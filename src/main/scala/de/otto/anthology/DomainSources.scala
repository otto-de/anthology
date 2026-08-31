package de.otto.anthology

import de.otto.anthology.MergeStage.*
import de.otto.anthology.SimpleThroughputLoggingStage.logThroughput
import de.otto.anthology.config.ChannelConfigs
import de.otto.anthology.kafka.ConsumerMap
import de.otto.anthology.kafka.ConsumerName
import de.otto.anthology.kafka.Passthrough
import ox.Ox
import ox.channels.BufferCapacity
import ox.flow.Flow

object DomainSources:

    def apply(
        configs: ChannelConfigs,
        consumers: ConsumerMap,
        logThroughput: Option[Boolean]
    )(using Ox): Flow[(Option[(QualifiedMessageId, Option[Message])], Passthrough)] =
        configs.channels
            .map: config =>
                // for now we go with consumer name == domain name
                val consumerName = ConsumerName(config.name.toString)
                val src = DomainSource(
                    config,
                    KafkaSourceSettings(config.kafka, consumerName, consumers(consumerName))
                )
                src.logThroughput(s"domain source ${config.name.toString}", logThroughput)
            .mergeFair()
            .logThroughput("domain sources total", logThroughput)
