package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.Message
import de.otto.anthology.MessageId
import de.otto.anthology.QualifiedMessageId
import de.otto.anthology.SimplePerformanceMeasureStage.measure
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.statestore.StateStoreSection
import de.otto.anthology.util.ExceptionUtil.stackTraceAsString
import org.rocksdb.RocksDBException
import ox.flow.Flow

import java.time.Instant
import scala.util.control.NonFatal

object DomainPersistenceStage extends LazyLogging:

    extension (in: Flow[(Option[(QualifiedMessageId, Option[Message])], Passthrough)])

        /** Persists incoming domain messages in the [[anthology.statestore.StateStore]]. A missing messages will be
          * treated as a deletion and removed from StateStore.
          */
        def persistDomainMessages(
            stateStore: StateStore,
            logThroughput: Option[Boolean] = None
        ): Flow[(Option[QualifiedMessageId], Passthrough)] =
            in.map:
                case (None, pass) =>
                    (Instant.now(), (None, pass))
                case (Some(qmid, messageOpt), pass) =>
                    val startingTime = Instant.now()
                    try
                        validateMessageId(qmid.id)
                        val messageKey: String = s"${StateStoreSection.DOM}/$qmid"
                        messageOpt match
                            case Some(message) =>
                                stateStore.putJson(messageKey, message.toJson)
                            case None =>
                                stateStore.delete(messageKey)
                        (startingTime, (Some(qmid), pass))
                    catch
                        case e: RocksDBException =>
                            throw e
                        case NonFatal(ex) =>
                            logger.error(
                                s"Error processing record (${pass.record.key}, ${pass.record.value}): ${ex.stackTraceAsString}"
                            )
                            (startingTime, (None, pass))
            .measure("DomainPersistence", logThroughput)

    private def validateMessageId(aid: MessageId): Unit =
        if aid.toString.contains(StateStore.ELEMENT_SEPARATOR) then
            throw new IllegalArgumentException(
                s"Message ids must not contain the '${StateStore.ELEMENT_SEPARATOR}' character"
            )
        if aid.toString.contains(StateStore.SEGMENT_SEPARATOR) then
            throw new IllegalArgumentException(
                s"Message ids must not contain the '${StateStore.SEGMENT_SEPARATOR}' character"
            )
