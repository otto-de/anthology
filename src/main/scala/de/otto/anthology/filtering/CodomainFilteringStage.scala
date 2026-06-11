package de.otto.anthology.filtering

import com.jayway.jsonpath.JsonPath
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.Parallelism
import de.otto.anthology.config.jsonPathConfigReader
import de.otto.anthology.kafka.Passthrough
import ox.flow.Flow
import pureconfig.ConfigReader

import scala.util.control.NonFatal

object CodomainFilteringStage extends LazyLogging:

    extension (in: Flow[(Seq[(AggregateId, Option[Aggregate])], Seq[Passthrough])])
        def filterCodomainAggregates(
            configOpt: Option[CodomainFilteringConfig],
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Seq[(AggregateId, Option[Aggregate])], Seq[Passthrough])] =
            val chain: FilterChain = FilterChain(configOpt.map(_.filterPaths).getOrElse(Seq.empty))
            in.mapPar(parallelism.toInt): (payloads, passthroughs) =>
                val payloadsOut: Seq[(AggregateId, Option[Aggregate])] =
                    payloads.map: (codomainAggregateId, codomainAggregate) =>
                        try (codomainAggregateId, chain(codomainAggregate))
                        catch
                            case NonFatal(ex) =>
                                logger.error(
                                    s"Error filtering ($codomainAggregateId, $codomainAggregate): ${ex.getMessage}"
                                )
                                (codomainAggregateId, None)
                (payloadsOut, passthroughs)

case class CodomainFilteringConfig(filterPaths: Seq[JsonPath]) derives ConfigReader
