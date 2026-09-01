package de.otto.anthology.config

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.ChannelName
import de.otto.anthology.MessageFormatName
import de.otto.anthology.config.jsonPathConfigReader
import pureconfig.ConfigReader
import pureconfig.generic.*
import pureconfig.generic.semiauto.deriveReader

sealed trait RelationConfig:
    def relFrom: (ChannelName, MessageFormatName)
    def relTo: (ChannelName, MessageFormatName)
    def refFromManyToOnePath: JsonPath

object RelationConfig:
    given FieldCoproductHint[RelationConfig] = FieldCoproductHint[RelationConfig]("type")
    given ConfigReader[RelationConfig] = deriveReader[RelationConfig]

given ConfigReader[(ChannelName, MessageFormatName)] =
    ConfigReader[String].map: c2mStr =>
        val c2m = c2mStr.split("/")
        (ChannelName(c2m(0)), MessageFormatName(c2m(1)))

case class OneToMany(
    relFrom: (ChannelName, MessageFormatName),
    relTo: (ChannelName, MessageFormatName),
    refFromManyToOnePath: JsonPath
) extends RelationConfig

case class ManyToOne(
    relFrom: (ChannelName, MessageFormatName),
    relTo: (ChannelName, MessageFormatName),
    refFromManyToOnePath: JsonPath,
    omitTriggerCodomain: Boolean = false
) extends RelationConfig
