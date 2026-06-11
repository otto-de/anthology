package de.otto.anthology.config

import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName
import de.otto.anthology.QualifiedAggregateId

case class DomainConfigs(domains: Seq[DomainConfig]):
    val domainsByName: Map[DomainName, DomainConfig] = domains.map(d => (d.name, d)).toMap
    val aggregatesByName: Map[(DomainName, AggregateName), AggregateConfig] =
        val list =
            domains.flatMap: d =>
                d.aggregates
                    .map: a =>
                        ((d.name, a.name), a)
        list.toMap

    def aggregateByQualifiedAggregateId(qaid: QualifiedAggregateId): AggregateConfig =
        aggregatesByName(qaid.domainName, qaid.aggregateName)
