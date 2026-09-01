package de.otto.anthology

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ValueNode
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option as JsonPathOption
import com.jayway.jsonpath.ParseContext
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.config.ChannelConfig
import de.otto.anthology.config.MessageFormatConfig
import de.otto.anthology.kafka.Passthrough
import ox.computeIntensive
import ox.flow.Flow

import java.util.Objects

object DomainSource extends LazyLogging:
    def apply(
        config: ChannelConfig,
        kafkaSettings: KafkaSourceSettings
    ): Flow[(Option[(QualifiedMessageId, Option[Message])], Passthrough)] =
        KafkaSource(kafkaSettings).map: pass =>
            val messageOpt: Option[Message] = Option(pass.record.value).flatten
            val messageConfigOpt = recogniseMessageConfig(messageOpt, config.messageFormats)
            messageConfigOpt match
                case None =>
                    logger.debug(
                        s"Unable to recognise message configuration for record (${pass.record.key}, ${pass.record.value})"
                    )
                    (None, pass)

                case Some(messageConfig) =>
                    val qmidOpt: Option[QualifiedMessageId] =
                        extractMessageId(messageOpt, pass.record.key, messageConfig)
                            .map(messageId => QualifiedMessageId(config.name, messageConfig.name, messageId))
                    qmidOpt match
                        case Some(qmid) =>
                            if messageConfig.logReceivedMessages.getOrElse(false) then
                                logger.info(
                                    s"Received domain message id=$qmid, msg=${messageOpt.map(_.toString).getOrElse("null")}"
                                )
                            (Some((qmid, messageOpt)), pass)
                        case None =>
                            (None, pass)

    private def recogniseMessageConfig(
        messageOpt: Option[Message],
        messageConfigs: Seq[MessageFormatConfig]
    ): Option[MessageFormatConfig] =
        if messageConfigs.size == 1 && messageConfigs.head.recognitionPath.isEmpty then messageConfigs.headOption
        else
            messageOpt match
                case None =>
                    None
                case Some(message) =>
                    messageConfigs.find: msgConfig =>
                        msgConfig.recognitionPath match
                            case Some(recPath) =>
                                val result: ArrayNode = jsonPathContext.parse(message.toJson).limit(1).read(recPath)
                                Objects.nonNull(result) && !result.isEmpty
                            case None =>
                                false

    private def extractMessageId(
        messageOpt: Option[Message],
        defaultMessageId: MessageId,
        messageConfig: MessageFormatConfig
    ): Option[MessageId] =
        messageConfig.idExtractionPath match
            case Some(extPath) =>
                messageOpt match
                    case Some(message) =>
                        computeIntensive:
                            Option(jsonPathContext.parse(message.toJson).read[ValueNode](extPath))
                                .map(v => if v.canConvertToLong then v.longValue else v.textValue)
                                .map(_.toString)
                                .map(MessageId(_))
                    case None =>
                        None
            case None =>
                Some(defaultMessageId)

    private val jsonPathContext: ParseContext = JsonPath.using(
        Configuration
            .builder()
            .jsonProvider(JacksonJsonNodeJsonProvider())
            .options(JsonPathOption.SUPPRESS_EXCEPTIONS) // When no match: null instead of exception
            .build()
    )
