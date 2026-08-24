package de.otto.anthology.headerpropagation

import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.Message
import de.otto.anthology.MessageId
import de.otto.anthology.SimpleProcessingTimeLogger.measureMap
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.util.ExceptionUtil.stackTraceAsString
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import ox.flow.Flow
import pureconfig.ConfigReader
import pureconfig.generic.FieldCoproductHint

import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.util.UUID
import scala.util.control.NonFatal

object HeaderPropagationStage extends LazyLogging:
    extension (in: Flow[(Seq[(MessageId, Option[Message])], Seq[Passthrough])])
        def propagateHeaders(
            configsOpt: Option[HeaderPropagationConfigs]
        ): Flow[(Seq[(MessageId, Option[Message], Option[Headers])], Seq[Passthrough])] =
            in.map:
                measureMap("HeaderPropagation"): (payloads, passthroughs) =>
                    val result =
                        configsOpt match
                            case Some(configs) =>
                                val payloadsOut: Seq[(MessageId, Option[Message], Option[Headers])] =
                                    payloads.map: (msgId, msgOpt) =>
                                        val headers = RecordHeaders()
                                        configs.headerPropagation.foreach: config =>
                                            try
                                                config match
                                                    case GenerateConstant(name, value) =>
                                                        headers.add(name, value.getBytes(StandardCharsets.UTF_8))
                                                    case GenerateUUID(name) =>
                                                        headers.add(
                                                            name,
                                                            UUID.randomUUID().toString.getBytes(StandardCharsets.UTF_8)
                                                        )
                                                    case GenerateTimestamp(name) =>
                                                        headers.add(
                                                            name,
                                                            ISO_OFFSET_DATE_TIME
                                                                .format(OffsetDateTime.now(ZoneId.of("UTC")))
                                                                .getBytes(StandardCharsets.UTF_8)
                                                        )
                                            catch
                                                case NonFatal(ex) =>
                                                    logger.error(
                                                        s"Error setting header for config $config: ${ex.stackTraceAsString}"
                                                    )
                                        (msgId, msgOpt, Some(headers))
                                (payloadsOut, passthroughs)
                            case None =>
                                val payloadsOut: Seq[(MessageId, Option[Message], Option[Headers])] =
                                    payloads.map: (msgId, msgOpt) =>
                                        (msgId, msgOpt, None)
                                (payloadsOut, passthroughs)
                    result

case class HeaderPropagationConfigs(headerPropagation: Seq[HeaderPropagationConfig])

sealed trait HeaderPropagationConfig derives ConfigReader:
    def name: String

object HeaderPropagationConfig:
    given FieldCoproductHint[HeaderPropagationConfig] = FieldCoproductHint[HeaderPropagationConfig]("type")

case class GenerateConstant(name: String, value: String) extends HeaderPropagationConfig
case class GenerateUUID(name: String) extends HeaderPropagationConfig
case class GenerateTimestamp(name: String) extends HeaderPropagationConfig
