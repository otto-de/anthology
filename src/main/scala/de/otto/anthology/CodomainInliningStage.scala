package de.otto.anthology

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.Parallelism
import de.otto.anthology.QualifiedAggregateId
import de.otto.anthology.config.DomainRelationConfigs
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.statestore.StateStoreSection
import ox.flow.Flow

import scala.util.control.NonFatal

/** Create codomain as nested structure.
  * {{{
  * {
  *     "foo": "bar",
  *     "domain-a": [
  *            {
  *                "bla": "bulbb",
  *                "domain-b": [ ... ]
  *            },
  *            {
  *                "bla": "lalelu",
  *                "domain-b": [ ... ]
  *            }
  *        ]
  * }
  * }}}
  */
object CodomainInliningStage extends LazyLogging:

    extension (in: Flow[(Seq[AggregateId], Seq[Passthrough])])
        def inlineDomainAggregates(
            config: DomainRelationConfigs,
            stateStore: StateStore,
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Seq[(AggregateId, Option[Aggregate])], Seq[Passthrough])] =
            in.mapPar(parallelism.toInt): (codomainAggregateIds, passthroughs) =>
                val results: Seq[(AggregateId, Option[Aggregate])] =
                    codomainAggregateIds.flatMap: codomainAggregateId =>
                        try
                            val codomainKeyStaged = s"${StateStoreSection.STA}/${codomainAggregateId.toString}"
                            stateStore
                                .getJson(codomainKeyStaged)
                                .map(_.asInstanceOf[ObjectNode])
                                .map: codomainAggregateStaged =>
                                    val codomainAggregateTemp: ObjectNode = mapper.createObjectNode()
                                    doInline(
                                        QualifiedAggregateId(config.root._1, config.root._2, codomainAggregateId),
                                        codomainAggregateTemp,
                                        codomainAggregateStaged,
                                        stateStore
                                    )
                                    val rootKey: String = s"${config.root._1}/${config.root._2}"
                                    val codomainAggregateOpt: Option[Aggregate] =
                                        Option(codomainAggregateTemp.get(rootKey))
                                            .map(_.asInstanceOf[ArrayNode])
                                            .map(_.get(0))
                                            .map(Aggregate(_))
                                    (codomainAggregateId, codomainAggregateOpt)
                        catch
                            case NonFatal(ex) =>
                                logger.error(
                                    s"Error processing codomain aggregate ($codomainAggregateId): ${ex.getMessage}"
                                )
                                None
                (results, passthroughs)

    private def doInline(
        currentDomainAggregateId: QualifiedAggregateId,
        parentAggregate: ObjectNode,
        codomainAggregateStaged: ObjectNode,
        stateStore: StateStore
    ): Unit =
        val currentDomainAggregateOpt: Option[ObjectNode] =
            Option(codomainAggregateStaged.get(currentDomainAggregateId.qualifierString))
                .map(_.asInstanceOf[ObjectNode])
                .flatMap(j => Option(j.get(currentDomainAggregateId.id.toString)).map(_.asInstanceOf[ObjectNode]))
                .map(_.deepCopy())

        currentDomainAggregateOpt.foreach: currentDomainAggregate =>
            val currentDomainAggregatesArray: ArrayNode =
                Option(parentAggregate.get(currentDomainAggregateId.qualifierString))
                    .fold {
                        val array = mapper.createArrayNode()
                        parentAggregate.set(currentDomainAggregateId.qualifierString, array)
                        array
                    }(_.asInstanceOf[ArrayNode])
            currentDomainAggregatesArray.add(currentDomainAggregate)

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
                doInline(nextAggregateId, currentDomainAggregate, codomainAggregateStaged, stateStore)
