package de.otto.anthology

final case class QualifiedMessageId(channelName: ChannelName, messageName: MessageFormatName, id: MessageId):
    lazy val qualifier: (ChannelName, MessageFormatName) = (channelName, messageName)
    lazy val qualifierString: String = s"$channelName/$messageName"
    override def toString(): String = s"$qualifierString/$id"
