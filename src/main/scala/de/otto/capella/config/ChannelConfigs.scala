package de.otto.capella.config

import de.otto.capella.ChannelName
import de.otto.capella.MessageFormatName
import de.otto.capella.QualifiedMessageId

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
