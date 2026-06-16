package de.otto.anthology.transformation

import com.fasterxml.jackson.core.`type`.TypeReference
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName
import de.otto.anthology.JsonSupport
import de.otto.anthology.Parallelism
import de.otto.anthology.QualifiedAggregateId
import de.otto.anthology.config.DomainConfigs
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.util.ExceptionUtil.stackTraceAsString
import io.joltcommunity.jolt.Chainr
import ox.flow.Flow
import pureconfig.ConfigReader

import java.io.FileInputStream
import java.io.InputStream
import scala.util.control.NonFatal
import scala.util.matching.Regex

object DomainTransformationStage extends LazyLogging:

    private val specTypeRef: TypeReference[java.util.List[Object]] = new TypeReference {}

    extension (in: Flow[(Option[(QualifiedAggregateId, Option[Aggregate])], Passthrough)])

        /** Transforms incoming domain aggregate ids based on a regex pattern, configured per domain. See
          * [[anthology.transformation.AggregateIdTransformer]] for details.
          */
        def transformDomainAggregateIds(
            configs: DomainConfigs
        ): Flow[(Option[(QualifiedAggregateId, Option[Aggregate])], Passthrough)] =
            in.map:
                case (None, pass) =>
                    (None, pass)
                case (Some(qaid, domainAggregateOpt), pass) =>
                    try
                        val configOpt = configs.aggregateByQualifiedAggregateId(qaid).idTransformation
                        val transformedDomainAggregateId =
                            configOpt match
                                case Some(config) =>
                                    AggregateIdTransformer(qaid.id, config.pattern)
                                case None =>
                                    qaid.id
                        (Some(qaid.copy(id = transformedDomainAggregateId), domainAggregateOpt), pass)
                    catch
                        case NonFatal(ex) =>
                            logger.error(
                                s"Error processing record (${pass.record.key}, ${pass.record.value}): ${ex.stackTraceAsString}"
                            )
                            (None, pass)

        /** Transforms incoming domain aggregates based on a [[https://jolt-community.github.io/jolt-community Jolt]]
          * spec, configured per domain.
          */
        def transformDomainAggregates(
            configs: DomainConfigs,
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Option[(QualifiedAggregateId, Option[Aggregate])], Passthrough)] =
            val specs: Map[(DomainName, AggregateName), Chainr] =
                configs.aggregatesByName.flatMap: (domName2aggName, aggConfig) =>
                    aggConfig.transformation.map: tc =>
                        val specInputStream: InputStream =
                            if tc.specFile.startsWith("/") then new FileInputStream(tc.specFile)
                            else classOf[DomainTransformationStage.type].getResourceAsStream("/" + tc.specFile)
                        val specJsonValue: java.util.List[Object] =
                            JsonSupport.mapper.readValue(specInputStream, specTypeRef)
                        val chain = Chainr.fromSpec(specJsonValue)
                        (domName2aggName, chain)

            in.mapPar(parallelism.toInt):
                case (None, pass) =>
                    (None, pass)
                case (Some(qaid, domainAggregateOpt), pass) =>
                    domainAggregateOpt match
                        case Some(domainAggregate) =>
                            val specOpt = specs.get((qaid.domainName, qaid.aggregateName))
                            val transformedDomainAggregate =
                                specOpt match
                                    case Some(spec) =>
                                        AggregateTransformer(domainAggregate, spec)
                                    case None =>
                                        domainAggregate
                            (Some(qaid, Some(transformedDomainAggregate)), pass)
                        case None =>
                            (Some(qaid, domainAggregateOpt), pass)

case class DomainAggregateIdTransformationConfig(pattern: Regex) derives ConfigReader

case class DomainAggregateTransformationConfig(specFile: String) derives ConfigReader
