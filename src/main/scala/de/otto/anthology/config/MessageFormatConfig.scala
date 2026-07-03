package de.otto.anthology.config

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.MessageFormatName
import de.otto.anthology.filtering.DomainFilteringConfig
import de.otto.anthology.transformation.DomainMessageIdTransformationConfig
import de.otto.anthology.transformation.DomainMessageTransformationConfig
import pureconfig.ConfigReader

// If the recognitionPath matches the message (== returns something), the message is recognised as being of this Message

case class MessageFormatConfig(
    name: MessageFormatName,
    recognitionPath: Option[JsonPath],
    filtering: Option[DomainFilteringConfig],
    idTransformation: Option[DomainMessageIdTransformationConfig],
    transformation: Option[DomainMessageTransformationConfig]
) derives ConfigReader
