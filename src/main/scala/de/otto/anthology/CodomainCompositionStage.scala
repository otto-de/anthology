package de.otto.anthology

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.AggregateId
import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.QualifiedAggregateId
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.statestore.StateStoreSection
import de.otto.anthology.util.ExceptionUtil.stackTraceAsString
import ox.flow.Flow

import scala.util.control.NonFatal

object CodomainCompositionStage extends LazyLogging:

    extension (in: Flow[(Seq[(QualifiedAggregateId, Seq[AggregateId])], Seq[Passthrough])])
        def composeCodomainAggregates(stateStore: StateStore): Flow[(Seq[AggregateId], Seq[Passthrough])] =
            in.map: (payloads, passthroughs) =>
                val payloadsOut: Seq[AggregateId] =
                    payloads.flatMap: (qaid, codomainAggregateIds) =>
                        codomainAggregateIds.flatMap: codomainAggregateId =>
                            try
                                val codomainKey = s"${StateStoreSection.STA}/$codomainAggregateId"
                                val codomainAggregate: ObjectNode =
                                    stateStore
                                        .getJson(codomainKey)
                                        .fold(mapper.createObjectNode())(_.asInstanceOf[ObjectNode])
                                compose(qaid, codomainAggregate, stateStore)
                                if codomainAggregate.isEmpty then stateStore.delete(codomainKey)
                                else stateStore.putJson(codomainKey, codomainAggregate)
                                Some(codomainAggregateId)
                            catch
                                case NonFatal(ex) =>
                                    logger.error(
                                        s"Error processing domain aggregate ($qaid) and codomain aggregate ($codomainAggregateId): ${ex.stackTraceAsString}"
                                    )
                                    None
                (payloadsOut.distinct, passthroughs)

    private def compose(
        currentDomainAggregateId: QualifiedAggregateId,
        codomainAggregate: ObjectNode,
        stateStore: StateStore
    ): Unit =
        val currentDomainAggregateOpt: Option[JsonNode] =
            stateStore.getJson(s"${StateStoreSection.DOM}/$currentDomainAggregateId")
        val currentDomainAggregateMap: ObjectNode =
            Option(codomainAggregate.get(currentDomainAggregateId.qualifierString))
                .fold(mapper.createObjectNode())(_.asInstanceOf[ObjectNode])
        setObject(currentDomainAggregateMap, currentDomainAggregateId.id.toString, currentDomainAggregateOpt)
        setObject(
            codomainAggregate,
            currentDomainAggregateId.qualifierString,
            if currentDomainAggregateMap.isEmpty then None else Some(currentDomainAggregateMap)
        )

        val next: Set[QualifiedAggregateId] =
            stateStore
                .getStringSet(
                    s"${StateStoreSection.LNK}/$currentDomainAggregateId"
                )
                .map: entry =>
                    val splittedEntry = entry.split("/")
                    QualifiedAggregateId(
                        DomainName(splittedEntry(0)),
                        AggregateName(splittedEntry(1)),
                        AggregateId(splittedEntry(2))
                    )
        next.foreach: nextAggregateId =>
            compose(nextAggregateId, codomainAggregate, stateStore)

    private def setObject(parent: ObjectNode, name: String, objOpt: Option[JsonNode]): Unit =
        objOpt match
            case Some(obj) =>
                parent.set[JsonNode](name, obj)
            case None =>
                parent.remove(name)
