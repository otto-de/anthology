package de.otto.capella.config

import de.otto.capella.ChannelName
import pureconfig.ConfigReader

case class DomainConfig(channels: Seq[ChannelConfig], relations: Seq[RelationConfig], logThroughput: Option[Boolean])
    derives ConfigReader:
    val channelsByName: Map[ChannelName, ChannelConfig] = channels.map(c => (c.name, c)).toMap
