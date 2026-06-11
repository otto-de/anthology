package de.otto.anthology.transformation

import com.fasterxml.jackson.core.`type`.TypeReference
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.Parallelism
import de.otto.anthology.kafka.Passthrough
import io.joltcommunity.jolt.Chainr
import ox.flow.Flow
import pureconfig.ConfigReader

import java.io.FileInputStream
import java.io.InputStream
import scala.util.control.NonFatal

object CodomainTransformationStage extends LazyLogging:

    private val specTypeRef: TypeReference[java.util.List[Object]] = new TypeReference {}

    extension (in: Flow[(Seq[(AggregateId, Option[Aggregate])], Seq[Passthrough])])

        /** Transforms outgoing codomain aggregates based on a [[https://jolt-community.github.io/jolt-community Jolt]]
          * spec.
          */
        def transformCodomainAggregates(
            configOpt: Option[CodomainTransformationConfig],
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Seq[(AggregateId, Option[Aggregate])], Seq[Passthrough])] =
            val specOpt: Option[Chainr] =
                configOpt.map: config =>
                    val specInputStream: InputStream =
                        if config.specFile.startsWith("/") then new FileInputStream(config.specFile)
                        else classOf[CodomainTransformationStage.type].getResourceAsStream("/" + config.specFile)
                    val specJsonValue: java.util.List[Object] = mapper.readValue(specInputStream, specTypeRef)
                    Chainr.fromSpec(specJsonValue)

            in.mapPar(parallelism.toInt): (payloads, passthroughs) =>
                val payloadsOut: Seq[(AggregateId, Option[Aggregate])] =
                    payloads.map: (codomainAggregateId, codomainAggregateOpt) =>
                        try
                            val transformedCodomainAggregateOpt: Option[Aggregate] =
                                (codomainAggregateOpt, specOpt) match
                                    case (Some(codomainAggregate), Some(spec)) =>
                                        Some(AggregateTransformer(codomainAggregate, spec))
                                    case _ =>
                                        codomainAggregateOpt
                            (codomainAggregateId, transformedCodomainAggregateOpt)
                        catch
                            case NonFatal(ex) =>
                                logger.error(
                                    s"Error transforming ($codomainAggregateId, $codomainAggregateOpt): ${ex.getMessage}"
                                )
                                (codomainAggregateId, None)
                (payloadsOut, passthroughs)

case class CodomainTransformationConfig(specFile: String) derives ConfigReader
