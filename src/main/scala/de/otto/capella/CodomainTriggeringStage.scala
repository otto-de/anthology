package de.otto.capella

import com.typesafe.scalalogging.LazyLogging
import de.otto.capella.ChannelName
import de.otto.capella.MessageFormatName
import de.otto.capella.MessageId
import de.otto.capella.Parallelism
import de.otto.capella.QualifiedMessageId
import de.otto.capella.SimpleProcessingTimeLogger.measureMap
import de.otto.capella.config.RelationConfigs
import de.otto.capella.kafka.Passthrough
import de.otto.capella.statestore.StateStore
import de.otto.capella.statestore.StateStoreSection
import de.otto.capella.util.ExceptionUtil.stackTraceAsString
import org.rocksdb.RocksDBException
import ox.flow.Flow

import scala.util.control.NonFatal

object CodomainTriggeringStage extends LazyLogging:

    // TODO currently we identify affected roots based on the new state.
    // - How can we identify roots of messages which lost references?
    // - How can we identify roots of deleted messages?

    extension (in: Flow[(Option[QualifiedMessageId], Passthrough)])

        def triggerAffectedCodomainMessages(
            config: RelationConfigs,
            stateStore: StateStore,
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Option[(QualifiedMessageId, Set[MessageId])], Passthrough)] =
            in.mapPar(parallelism.toInt):
                measureMap("CodomainTriggering"):
                    case (None, pass) =>
                        (None, pass)
                    case (Some(qmid), pass) =>
                        try
                            val rootIds = identifyAffected(qmid, config, stateStore)
                            (Some(qmid, rootIds), pass)
                        catch
                            case e: RocksDBException =>
                                throw e
                            case NonFatal(ex) =>
                                logger.error(
                                    s"Error processing record (${pass.record.key}, ${pass.record.value}): ${ex.stackTraceAsString}"
                                )
                                (None, pass)

    private def identifyAffected(
        currentDomainMessageId: QualifiedMessageId,
        config: RelationConfigs,
        stateStore: StateStore
    ): Set[MessageId] =
        if currentDomainMessageId.qualifier == config.root then Set(currentDomainMessageId.id)
        else
            val next: Set[QualifiedMessageId] =
                stateStore
                    .getStringSet(s"${StateStoreSection.BLK}/$currentDomainMessageId")
                    .map: entry =>
                        val splittedEntry = entry.split("/")
                        QualifiedMessageId(
                            ChannelName(splittedEntry(0)),
                            MessageFormatName(splittedEntry(1)),
                            MessageId(splittedEntry(2))
                        )
            next.flatMap: nextMessageId =>
                identifyAffected(nextMessageId, config, stateStore)
