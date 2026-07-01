package de.otto.anthology

import com.fasterxml.jackson.databind.JsonNode
import pureconfig.ConfigReader

opaque type MessageId = String
object MessageId:
    def apply(mid: String): MessageId = mid

opaque type Message = JsonNode
object Message:
    def apply(agg: JsonNode): Message = agg
    extension (agg: Message) def toJson: JsonNode = agg

/** Name of a source channel.
  */
opaque type ChannelName = String
object ChannelName:
    def apply(name: String): ChannelName = name
    given ConfigReader[ChannelName] = ConfigReader[String].map(nameStr => ChannelName(nameStr))

/** Name of an message format. Unlike the [[anthology.MessageId]], it does not identify an instance, but rather a class
  * of messages.
  */
opaque type MessageFormatName = String
object MessageFormatName:
    def apply(name: String): MessageFormatName = name
    given ConfigReader[MessageFormatName] = ConfigReader[String].map(nameStr => MessageFormatName(nameStr))

opaque type Parallelism = Int
object Parallelism:
    def apply(para: Int): Parallelism = para
    given ConfigReader[Parallelism] = ConfigReader[Int].map(paraInt => Parallelism(paraInt))
    extension (para: Parallelism) def toInt: Int = para
