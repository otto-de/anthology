package de.otto.anthology.config

import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName

case class DomainRelationConfigs(relations: Seq[DomainRelationConfig]):
    val manyToOneRelationsStartingFrom: Map[(DomainName, AggregateName), Set[ManyToOne]] =
        val result = scala.collection.mutable.Map[(DomainName, AggregateName), Set[ManyToOne]]()
        relations
            .collect:
                case mto: ManyToOne =>
                    val oldSet = result.getOrElse(mto.relFrom, Set.empty)
                    val newSet = oldSet + mto
                    result.update(mto.relFrom, newSet)
        result.toMap

    val oneToManyRelationsLeadingTo: Map[(DomainName, AggregateName), Set[OneToMany]] =
        val result = scala.collection.mutable.Map[(DomainName, AggregateName), Set[OneToMany]]()
        relations
            .collect:
                case otm: OneToMany =>
                    val oldSet = result.getOrElse(otm.relTo, Set.empty)
                    val newSet = oldSet + otm
                    result.update(otm.relTo, newSet)
        result.toMap

    val root: (DomainName, AggregateName) =
        val all: Set[(DomainName, AggregateName)] =
            relations.flatMap(rel => Seq(rel.relFrom, rel.relTo)).toSet
        val to: Set[(DomainName, AggregateName)] = relations.map(rel => rel.relTo).toSet
        val roots: Set[(DomainName, AggregateName)] = all -- to
        if roots.isEmpty then throw new IllegalArgumentException("Possible misconfiguration: no root configured")
        else if roots.size > 1 then
            throw new IllegalArgumentException(
                s"Possible misconfiguration: multiple roots (${roots.mkString}) configured"
            )
        else roots.head
