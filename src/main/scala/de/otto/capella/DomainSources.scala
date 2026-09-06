package de.otto.capella

import de.otto.capella.MergeStage.*
import de.otto.capella.SimpleThroughputLoggingStage.logThroughput
import de.otto.capella.config.ChannelConfigs
import de.otto.capella.kafka.ConsumerMap
import de.otto.capella.kafka.ConsumerName
import de.otto.capella.kafka.Passthrough
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
