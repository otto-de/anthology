package de.otto.capella

import com.typesafe.scalalogging.LazyLogging
import de.otto.capella.Message
import de.otto.capella.MessageId
import de.otto.capella.QualifiedMessageId
import de.otto.capella.SimpleProcessingTimeLogger.measureMap
import de.otto.capella.kafka.Passthrough
import de.otto.capella.statestore.StateStore
import de.otto.capella.statestore.StateStoreSection
import de.otto.capella.util.ExceptionUtil.stackTraceAsString
import org.rocksdb.RocksDBException
import ox.flow.Flow

import scala.util.control.NonFatal

object DomainPersistenceStage extends LazyLogging:

    extension (in: Flow[(Option[(QualifiedMessageId, Option[Message])], Passthrough)])

        /** Persists incoming domain messages in the [[capella.statestore.StateStore]]. A missing messages will be
          * treated as a deletion and removed from StateStore.
          */
        def persistDomainMessages(
            stateStore: StateStore
        ): Flow[(Option[QualifiedMessageId], Passthrough)] =
            in.map:
                measureMap("DomainPersistence"):
                    case (None, pass) =>
                        (None, pass)
                    case (Some(qmid, messageOpt), pass) =>
                        try
                            validateMessageId(qmid.id)
                            val messageKey: String = s"${StateStoreSection.DOM}/$qmid"
                            messageOpt match
                                case Some(message) =>
                                    stateStore.putJson(messageKey, message.toJson)
                                case None =>
                                    stateStore.delete(messageKey)
                            (Some(qmid), pass)
                        catch
                            case e: RocksDBException =>
                                throw e
                            case NonFatal(ex) =>
                                logger.error(
                                    s"Error processing record (${pass.record.key}, ${pass.record.value}): ${ex.stackTraceAsString}"
                                )
                                (None, pass)

    private def validateMessageId(aid: MessageId): Unit =
        if aid.toString.contains(StateStore.ELEMENT_SEPARATOR) then
            throw new IllegalArgumentException(
                s"Message ids must not contain the '${StateStore.ELEMENT_SEPARATOR}' character"
            )
        if aid.toString.contains(StateStore.SEGMENT_SEPARATOR) then
            throw new IllegalArgumentException(
                s"Message ids must not contain the '${StateStore.SEGMENT_SEPARATOR}' character"
            )
