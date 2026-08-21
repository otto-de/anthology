package de.otto.anthology.transformation

import com.fasterxml.jackson.core.`type`.TypeReference
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.Message
import de.otto.anthology.MessageId
import de.otto.anthology.Parallelism
import de.otto.anthology.SimpleProcessingTimeLogger.measure
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.util.ExceptionUtil.stackTraceAsString
import io.joltcommunity.jolt.Chainr
import ox.flow.Flow
import pureconfig.ConfigReader

import java.io.FileInputStream
import java.io.InputStream
import scala.util.control.NonFatal

object CodomainTransformationStage extends LazyLogging:

    private val specTypeRef: TypeReference[java.util.List[Object]] = new TypeReference {}

    extension (in: Flow[(Seq[(MessageId, Option[Message])], Seq[Passthrough])])

        /** Transforms outgoing codomain messages based on a [[https://jolt-community.github.io/jolt-community Jolt]]
          * spec.
          */
        def transformCodomainMessages(
            configOpt: Option[CodomainTransformationConfig],
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Seq[(MessageId, Option[Message])], Seq[Passthrough])] =
            val specOpt: Option[Chainr] =
                configOpt.map: config =>
                    val specInputStream: InputStream =
                        if config.specFile.startsWith("/") then new FileInputStream(config.specFile)
                        else classOf[CodomainTransformationStage.type].getResourceAsStream("/" + config.specFile)
                    val specJsonValue: java.util.List[Object] = mapper.readValue(specInputStream, specTypeRef)
                    Chainr.fromSpec(specJsonValue)

            in.mapPar(parallelism.toInt):
                measure("CodomainTransformation"): (payloads, passthroughs) =>
                    val payloadsOut: Seq[(MessageId, Option[Message])] =
                        payloads.map: (codomainMessageId, codomainMessageOpt) =>
                            try
                                val transformedCodomainMessageOpt: Option[Message] =
                                    (codomainMessageOpt, specOpt) match
                                        case (Some(codomainMessage), Some(spec)) =>
                                            Some(MessageTransformer(codomainMessage, spec))
                                        case _ =>
                                            codomainMessageOpt
                                (codomainMessageId, transformedCodomainMessageOpt)
                            catch
                                case NonFatal(ex) =>
                                    logger.error(
                                        s"Error transforming ($codomainMessageId, $codomainMessageOpt): ${ex.stackTraceAsString}"
                                    )
                                    (codomainMessageId, None)
                    (payloadsOut, passthroughs)

case class CodomainTransformationConfig(specFile: String) derives ConfigReader
