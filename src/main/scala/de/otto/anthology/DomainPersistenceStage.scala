package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.QualifiedAggregateId
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.statestore.StateStoreSection
import de.otto.anthology.util.ExceptionUtil.stackTraceAsString
import ox.flow.Flow

import scala.util.control.NonFatal

object DomainPersistenceStage extends LazyLogging:

    extension (in: Flow[(Option[(QualifiedAggregateId, Option[Aggregate])], Passthrough)])

        /** Persists incoming Domain Aggregates in the [[anthology.statestore.StateStore]]. A missing Aggregate will be
          * treated as a deletion and removed from StateStore.
          */
        def persistDomainAggregates(stateStore: StateStore): Flow[(Option[QualifiedAggregateId], Passthrough)] =
            in.map:
                case (None, pass) =>
                    (None, pass)
                case (Some(qaid, aggregateOpt), pass) =>
                    try
                        validateAggregateId(qaid.id)
                        val aggregateKey: String = s"${StateStoreSection.DOM}/$qaid"
                        aggregateOpt match
                            case Some(aggregate) =>
                                stateStore.putJson(aggregateKey, aggregate.toJson)
                            case None =>
                                stateStore.delete(aggregateKey)
                        (Some(qaid), pass)
                    catch
                        case NonFatal(ex) =>
                            logger.error(
                                s"Error processing record (${pass.record.key}, ${pass.record.value}): ${ex.stackTraceAsString}"
                            )
                            (None, pass)

    private def validateAggregateId(aid: AggregateId): Unit =
        if aid.toString.contains(StateStore.ELEMENT_SEPARATOR) then
            throw new IllegalArgumentException(
                s"Aggregate ids must not contain the '${StateStore.ELEMENT_SEPARATOR}' character"
            )
        if aid.toString.contains(StateStore.SEGMENT_SEPARATOR) then
            throw new IllegalArgumentException(
                s"Aggregate ids must not contain the '${StateStore.SEGMENT_SEPARATOR}' character"
            )
