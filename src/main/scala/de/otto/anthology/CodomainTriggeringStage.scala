package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.AggregateId
import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName
import de.otto.anthology.Parallelism
import de.otto.anthology.QualifiedAggregateId
import de.otto.anthology.config.DomainRelationConfigs
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.statestore.StateStoreSection
import ox.flow.Flow

import scala.util.control.NonFatal

object CodomainTriggeringStage extends LazyLogging:

    // TODO currently we identify affected root agregates based on the new state.
    // - How can we identify roots of aggregates which lost references?
    // - How can we identify roots of deleted aggregates?

    extension (in: Flow[(Option[QualifiedAggregateId], Passthrough)])

        def triggerAffectedCodomainAggregates(
            config: DomainRelationConfigs,
            stateStore: StateStore,
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Option[(QualifiedAggregateId, Set[AggregateId])], Passthrough)] =
            in.mapPar(parallelism.toInt):
                case (None, pass) =>
                    (None, pass)
                case (Some(qaid), pass) =>
                    try
                        val aggregateRootIds = identify(qaid, config, stateStore)
                        (Some(qaid, aggregateRootIds), pass)
                    catch
                        case NonFatal(ex) =>
                            logger.error(
                                s"Error processing record (${pass.record.key}, ${pass.record.value}): ${ex.getMessage}"
                            )
                            (None, pass)

    private def identify(
        currentDomainAggregateId: QualifiedAggregateId,
        config: DomainRelationConfigs,
        stateStore: StateStore
    ): Set[AggregateId] =
        if currentDomainAggregateId.qualifier == config.root then Set(currentDomainAggregateId.id)
        else
            val next: Set[QualifiedAggregateId] =
                stateStore
                    .getStringSet(s"${StateStoreSection.BLK}/$currentDomainAggregateId")
                    .map: entry =>
                        val splittedEntry = entry.split("/")
                        QualifiedAggregateId(
                            DomainName(splittedEntry(0)),
                            AggregateName(splittedEntry(1)),
                            AggregateId(splittedEntry(2))
                        )
            next.flatMap: nextAggregateId =>
                identify(nextAggregateId, config, stateStore)
