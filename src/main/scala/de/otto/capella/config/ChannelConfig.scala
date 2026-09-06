package de.otto.capella.config

import de.otto.capella.ChannelName
import de.otto.capella.KafkaSourceConfig
import de.otto.capella.MessageFormatName
import pureconfig.ConfigReader

case class ChannelConfig(name: ChannelName, kafka: KafkaSourceConfig, messageFormats: Seq[MessageFormatConfig])
    derives ConfigReader:

    val messageFormatsByName: Map[MessageFormatName, MessageFormatConfig] = messageFormats.map(m => (m.name, m)).toMap
