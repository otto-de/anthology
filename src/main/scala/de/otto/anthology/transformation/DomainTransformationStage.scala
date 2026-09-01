package de.otto.anthology.transformation

import com.fasterxml.jackson.core.`type`.TypeReference
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.ChannelName
import de.otto.anthology.JsonSupport
import de.otto.anthology.Message
import de.otto.anthology.MessageFormatName
import de.otto.anthology.Parallelism
import de.otto.anthology.QualifiedMessageId
import de.otto.anthology.SimpleProcessingTimeLogger.measureMap
import de.otto.anthology.config.ChannelConfigs
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

    extension (in: Flow[(Option[(QualifiedMessageId, Option[Message])], Passthrough)])

        /** Transforms incoming domain message ids based on a regex pattern, configured per channel. See
          * [[anthology.transformation.MessageIdTransformer]] for details.
          */
        def transformDomainMessageIds(
            configs: ChannelConfigs
        ): Flow[(Option[(QualifiedMessageId, Option[Message])], Passthrough)] =
            in.map:
                measureMap("DomainTransformationIds"):
                    case (None, pass) =>
                        (None, pass)
                    case (Some(qmid, domainMessageOpt), pass) =>
                        try
                            val configOpt = configs.messageFormatByQualifiedMessageId(qmid).idTransformation
                            val transformedDomainMessageId =
                                configOpt match
                                    case Some(config) =>
                                        MessageIdTransformer(qmid.id, config.pattern)
                                    case None =>
                                        qmid.id
                            (Some(qmid.copy(id = transformedDomainMessageId), domainMessageOpt), pass)
                        catch
                            case NonFatal(ex) =>
                                logger.error(
                                    s"Error processing record (${pass.record.key}, ${pass.record.value}): ${ex.stackTraceAsString}"
                                )
                                (None, pass)

        /** Transforms incoming domain messages based on a [[https://jolt-community.github.io/jolt-community Jolt]]
          * spec, configured per channel.
          */
        def transformDomainMessages(
            configs: ChannelConfigs,
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Option[(QualifiedMessageId, Option[Message])], Passthrough)] =
            val specs: Map[(ChannelName, MessageFormatName), Chainr] =
                configs.messageFormatsByName.flatMap: (chanName2msgName, msgConfig) =>
                    msgConfig.transformation.map: tc =>
                        val specInputStream: InputStream =
                            if tc.specFile.startsWith("/") then new FileInputStream(tc.specFile)
                            else classOf[DomainTransformationStage.type].getResourceAsStream("/" + tc.specFile)
                        val specJsonValue: java.util.List[Object] =
                            JsonSupport.mapper.readValue(specInputStream, specTypeRef)
                        val chain = Chainr.fromSpec(specJsonValue)
                        (chanName2msgName, chain)

            in.mapPar(parallelism.toInt):
                measureMap("DomainTransformationMsgs"):
                    case (None, pass) =>
                        (None, pass)
                    case (Some(qmid, domainMessageOpt), pass) =>
                        domainMessageOpt match
                            case Some(domainMessage) =>
                                val specOpt = specs.get((qmid.channelName, qmid.messageName))
                                val transformedDomainMessage =
                                    specOpt match
                                        case Some(spec) =>
                                            MessageTransformer(domainMessage, spec)
                                        case None =>
                                            domainMessage
                                (Some(qmid, Some(transformedDomainMessage)), pass)
                            case None =>
                                (Some(qmid, domainMessageOpt), pass)

case class DomainMessageIdTransformationConfig(pattern: Regex) derives ConfigReader

case class DomainMessageTransformationConfig(specFile: String) derives ConfigReader
