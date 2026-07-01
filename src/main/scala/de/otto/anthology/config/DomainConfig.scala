package de.otto.anthology.config

import de.otto.anthology.ChannelName
import pureconfig.ConfigReader

case class DomainConfig(channels: Seq[ChannelConfig], relations: Seq[RelationConfig]) derives ConfigReader:
    val channelsByName: Map[ChannelName, ChannelConfig] = channels.map(c => (c.name, c)).toMap
