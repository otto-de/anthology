package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.statestore.StateStoreSection
import de.otto.anthology.util.ExceptionUtil.stackTraceAsString
import ox.flow.Flow
import ox.mapPar

import scala.util.control.NonFatal

object CodomainPersistenceStage extends LazyLogging:

    extension (in: Flow[(Seq[(AggregateId, Option[Aggregate])], Seq[Passthrough])])

        /** Persists outgoing Codomain Aggregates in the [[anthology.statestore.StateStore]]. A missing Aggregate will
          * be treated as a deletion and removed from StateStore.
          */
        def persistCodomainAggregates(
            stateStore: StateStore,
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Seq[(AggregateId, Option[Aggregate])], Seq[Passthrough])] =
            in.map: (payloads, passthroughs) =>
                val payloadsOut = payloads.mapPar(parallelism.toInt): aggId2agg =>
                    try
                        val aggregateKey: String = s"${StateStoreSection.COD}/${aggId2agg._1}"
                        aggId2agg._2 match
                            case Some(aggregate) =>
                                stateStore.putJson(aggregateKey, aggregate.toJson)
                            case None =>
                                stateStore.delete(aggregateKey)
                        aggId2agg
                    catch
                        case NonFatal(ex) =>
                            logger.error(
                                s"Error persisting codomain aggregate (${aggId2agg._1}, ${aggId2agg._2}): ${ex.stackTraceAsString}"
                            )
                            (aggId2agg._1, None)
                (payloadsOut, passthroughs)
