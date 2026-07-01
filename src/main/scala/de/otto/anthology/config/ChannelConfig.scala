package de.otto.anthology.config

import de.otto.anthology.ChannelName
import de.otto.anthology.KafkaSourceConfig
import de.otto.anthology.MessageFormatName
import pureconfig.ConfigReader

case class ChannelConfig(name: ChannelName, kafka: KafkaSourceConfig, messageFormats: Seq[MessageFormatConfig])
    derives ConfigReader:

    val messageFormatsByName: Map[MessageFormatName, MessageFormatConfig] = messageFormats.map(m => (m.name, m)).toMap
