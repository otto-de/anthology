package de.otto.capella.config

import de.otto.capella.ChannelName
import de.otto.capella.MessageFormatName

case class RelationConfigs(relations: Seq[RelationConfig]):
    val manyToOneRelationsStartingFrom: Map[(ChannelName, MessageFormatName), Set[ManyToOne]] =
        val result = scala.collection.mutable.Map[(ChannelName, MessageFormatName), Set[ManyToOne]]()
        relations
            .collect:
                case mto: ManyToOne =>
                    val oldSet = result.getOrElse(mto.relFrom, Set.empty)
                    val newSet = oldSet + mto
                    result.update(mto.relFrom, newSet)
        result.toMap

    val oneToManyRelationsLeadingTo: Map[(ChannelName, MessageFormatName), Set[OneToMany]] =
        val result = scala.collection.mutable.Map[(ChannelName, MessageFormatName), Set[OneToMany]]()
        relations
            .collect:
                case otm: OneToMany =>
                    val oldSet = result.getOrElse(otm.relTo, Set.empty)
                    val newSet = oldSet + otm
                    result.update(otm.relTo, newSet)
        result.toMap

    val root: (ChannelName, MessageFormatName) =
        val all: Set[(ChannelName, MessageFormatName)] =
            relations.flatMap(rel => Seq(rel.relFrom, rel.relTo)).toSet
        val to: Set[(ChannelName, MessageFormatName)] = relations.map(rel => rel.relTo).toSet
        val roots: Set[(ChannelName, MessageFormatName)] = all -- to
        if roots.isEmpty then throw new IllegalArgumentException("Possible misconfiguration: no root configured")
        else if roots.size > 1 then
            throw new IllegalArgumentException(
                s"Possible misconfiguration: multiple roots (${roots.mkString}) configured"
            )
        else roots.head
