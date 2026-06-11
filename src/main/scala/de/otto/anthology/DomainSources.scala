package de.otto.anthology

import de.otto.anthology.MergeStage.*
import de.otto.anthology.SimpleLoggingCounterStage.count
import de.otto.anthology.config.DomainConfigs
import de.otto.anthology.kafka.ConsumerMap
import de.otto.anthology.kafka.ConsumerName
import de.otto.anthology.kafka.Passthrough
import ox.Ox
import ox.channels.BufferCapacity
import ox.flow.Flow

object DomainSources:

    def apply(
        configs: DomainConfigs,
        consumers: ConsumerMap
    )(using Ox): Flow[(Option[(QualifiedAggregateId, Option[Aggregate])], Passthrough)] =
        configs.domains
            .map: config =>
                // for now we go with consumer name == domain name
                val consumerName = ConsumerName(config.name.toString)
                DomainSource(
                    config,
                    KafkaSourceSettings(config.kafka, consumerName, consumers(consumerName))
                ).count(s"domain source ${config.name.toString}")
            .mergeFair()
