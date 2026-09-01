package de.otto.anthology

final case class QualifiedMessageId(channelName: ChannelName, messageName: MessageFormatName, id: MessageId):
    lazy val qualifier: (ChannelName, MessageFormatName) = (channelName, messageName)
    lazy val qualifierString: String = s"$channelName/$messageName"
    override def toString(): String = s"$qualifierString/$id"

object QualifiedMessageId:
    def apply(qmidStr: String): QualifiedMessageId =
        val splitted: Array[String] = qmidStr.split("/")
        assert(splitted.size == 3, "Invalid format")
        QualifiedMessageId(ChannelName(splitted(0)), MessageFormatName(splitted(1)), MessageId(splitted(2)))

    def apply(qualifier: (ChannelName, MessageFormatName), id: MessageId): QualifiedMessageId =
        QualifiedMessageId(qualifier._1, qualifier._2, id)
