package de.otto.capella.config

import com.jayway.jsonpath.JsonPath
import de.otto.capella.MessageFormatName
import de.otto.capella.filtering.DomainFilteringConfig
import de.otto.capella.transformation.DomainMessageIdTransformationConfig
import de.otto.capella.transformation.DomainMessageTransformationConfig
import pureconfig.ConfigReader

// If the recognitionPath matches the message (== returns something), the message is recognised as being of this Message

case class MessageFormatConfig(
    name: MessageFormatName,
    recognitionPath: Option[JsonPath],
    idExtractionPath: Option[JsonPath],
    filtering: Option[DomainFilteringConfig],
    idTransformation: Option[DomainMessageIdTransformationConfig],
    transformation: Option[DomainMessageTransformationConfig],
    logReceivedMessages: Option[Boolean]
) derives ConfigReader
