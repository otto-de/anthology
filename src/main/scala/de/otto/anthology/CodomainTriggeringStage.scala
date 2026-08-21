package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.ChannelName
import de.otto.anthology.MessageFormatName
import de.otto.anthology.MessageId
import de.otto.anthology.Parallelism
import de.otto.anthology.QualifiedMessageId
import de.otto.anthology.SimpleProcessingTimeLogger.measure
import de.otto.anthology.config.RelationConfigs
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.statestore.StateStoreSection
import de.otto.anthology.util.ExceptionUtil.stackTraceAsString
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
                measure("CodomainTriggering"):
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
