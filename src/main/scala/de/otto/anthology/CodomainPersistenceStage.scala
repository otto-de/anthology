package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.Message
import de.otto.anthology.MessageId
import de.otto.anthology.SimpleProcessingTimeLogger.measure
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.statestore.StateStoreSection
import de.otto.anthology.util.ExceptionUtil.stackTraceAsString
import org.rocksdb.RocksDBException
import ox.flow.Flow
import ox.mapPar

import scala.util.control.NonFatal

object CodomainPersistenceStage extends LazyLogging:

    extension (in: Flow[(Seq[(MessageId, Option[Message])], Seq[Passthrough])])

        /** Persists outgoing codomain messages in the [[anthology.statestore.StateStore]]. A missing message will be
          * treated as a deletion and removed from StateStore.
          */
        def persistCodomainMessages(
            stateStore: StateStore,
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Seq[(MessageId, Option[Message])], Seq[Passthrough])] =
            in.map:
                measure("CodomainPersistence"): (payloads, passthroughs) =>
                    val payloadsOut = payloads.mapPar(parallelism.toInt): msgId2msg =>
                        try
                            val messageKey: String = s"${StateStoreSection.COD}/${msgId2msg._1}"
                            msgId2msg._2 match
                                case Some(message) =>
                                    stateStore.putJson(messageKey, message.toJson)
                                case None =>
                                    stateStore.delete(messageKey)
                            msgId2msg
                        catch
                            case e: RocksDBException =>
                                throw e
                            case NonFatal(ex) =>
                                logger.error(
                                    s"Error persisting codomain message (${msgId2msg._1}, ${msgId2msg._2}): ${ex.stackTraceAsString}"
                                )
                                (msgId2msg._1, None)
                    (payloadsOut, passthroughs)
