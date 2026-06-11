package de.otto.anthology.config

import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName

case class DomainRelationConfigs(relations: Seq[DomainRelationConfig]):
    val manyToOneRelationsStartingFrom: Map[(DomainName, AggregateName), Set[ManyToOneConfig]] =
        val result = scala.collection.mutable.Map[(DomainName, AggregateName), Set[ManyToOneConfig]]()
        relations
            .collect:
                case mto: ManyToOneConfig =>
                    val oldSet = result.getOrElse(mto.from, Set.empty)
                    val newSet = oldSet + mto
                    result.update(mto.from, newSet)
        result.toMap

    val oneToManyRelationsLeadingTo: Map[(DomainName, AggregateName), Set[OneToManyConfig]] =
        val result = scala.collection.mutable.Map[(DomainName, AggregateName), Set[OneToManyConfig]]()
        relations
            .collect:
                case otm: OneToManyConfig =>
                    val oldSet = result.getOrElse(otm.to, Set.empty)
                    val newSet = oldSet + otm
                    result.update(otm.to, newSet)
        result.toMap

    val root: (DomainName, AggregateName) =
        val all: Set[(DomainName, AggregateName)] = relations.flatMap(rel => Seq(rel.from, rel.to)).toSet
        val to: Set[(DomainName, AggregateName)] = relations.map(rel => rel.to).toSet
        val roots: Set[(DomainName, AggregateName)] = all -- to
        if roots.isEmpty then throw new IllegalArgumentException("Possible misconfiguration: no root configured")
        else if roots.size > 1 then
            throw new IllegalArgumentException(
                s"Possible misconfiguration: multiple roots (${roots.mkString}) configured"
            )
        else roots.head
