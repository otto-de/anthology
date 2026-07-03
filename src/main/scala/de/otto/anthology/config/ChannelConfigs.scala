package de.otto.anthology.config

import de.otto.anthology.ChannelName
import de.otto.anthology.MessageFormatName
import de.otto.anthology.QualifiedMessageId

case class ChannelConfigs(channels: Seq[ChannelConfig]):
    val channelsByName: Map[ChannelName, ChannelConfig] = channels.map(c => (c.name, c)).toMap
    val messageFormatsByName: Map[(ChannelName, MessageFormatName), MessageFormatConfig] =
        val list =
            channels.flatMap: c =>
                c.messageFormats
                    .map: m =>
                        ((c.name, m.name), m)
        list.toMap

    def messageFormatByQualifiedMessageId(qmid: QualifiedMessageId): MessageFormatConfig =
        messageFormatsByName(qmid.channelName, qmid.messageName)
