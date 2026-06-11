package de.otto.anthology.config

import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName
import de.otto.anthology.KafkaSourceConfig
import pureconfig.ConfigReader

case class DomainConfig(name: DomainName, kafka: KafkaSourceConfig, aggregates: Seq[AggregateConfig])
    derives ConfigReader:

    val aggregatesByName: Map[AggregateName, AggregateConfig] = aggregates.map(a => (a.name, a)).toMap
