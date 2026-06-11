package de.otto.anthology.filtering

import com.jayway.jsonpath.JsonPath
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName
import de.otto.anthology.Parallelism
import de.otto.anthology.QualifiedAggregateId
import de.otto.anthology.config.DomainConfigs
import de.otto.anthology.config.jsonPathConfigReader
import de.otto.anthology.kafka.Passthrough
import ox.flow.Flow
import pureconfig.ConfigReader

import scala.util.control.NonFatal

object DomainFilteringStage extends LazyLogging:

    extension (in: Flow[(Option[(QualifiedAggregateId, Option[Aggregate])], Passthrough)])
        def filterDomainAggregates(
            configs: DomainConfigs,
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Option[(QualifiedAggregateId, Option[Aggregate])], Passthrough)] =
            val chains: Map[(DomainName, AggregateName), FilterChain] =
                configs.aggregatesByName.flatMap: (domName2aggName, aggConfig) =>
                    aggConfig.filtering.map(fc => (domName2aggName, FilterChain(fc.filterPaths)))

            in.mapPar(parallelism.toInt):
                case (None, pass) =>
                    (None, pass)

                case (Some(qaid, aggregateOpt), pass) =>
                    try
                        val chainOpt = chains.get((qaid.domainName, qaid.aggregateName))
                        val filteredDomainAggregate =
                            chainOpt match
                                case Some(chain) =>
                                    chain(aggregateOpt)
                                case None =>
                                    aggregateOpt
                        (Some(qaid, filteredDomainAggregate), pass)
                    catch
                        case NonFatal(ex) =>
                            logger.error(
                                s"Error processing record (${pass.record.key}, ${pass.record.value}): ${ex.getMessage}"
                            )
                            (None, pass)

case class DomainFilteringConfig(filterPaths: Seq[JsonPath]) derives ConfigReader
