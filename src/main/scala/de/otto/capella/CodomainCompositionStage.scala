package de.otto.capella

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging
import de.otto.capella.ChannelName
import de.otto.capella.JsonSupport.mapper
import de.otto.capella.MessageFormatName
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

object CodomainCompositionStage extends LazyLogging:

    extension (in: Flow[(Seq[(QualifiedMessageId, Seq[MessageId])], Seq[Passthrough])])
        def composeCodomainMessages(
            stateStore: StateStore
        ): Flow[(Seq[MessageId], Seq[Passthrough])] =
            in.map:
                measureMap("CodomainComposition"): (payloads, passthroughs) =>
                    val payloadsOut: Seq[MessageId] =
                        payloads.flatMap: (qmid, codomainMessageIds) =>
                            codomainMessageIds.flatMap: codomainMessageId =>
                                try
                                    val codomainKey = s"${StateStoreSection.STA}/$codomainMessageId"
                                    val codomainMessage: ObjectNode =
                                        stateStore
                                            .getJson(codomainKey)
                                            .fold(mapper.createObjectNode())(_.asInstanceOf[ObjectNode])
                                    compose(qmid, codomainMessage, stateStore)
                                    if codomainMessage.isEmpty then stateStore.delete(codomainKey)
                                    else stateStore.putJson(codomainKey, codomainMessage)
                                    Some(codomainMessageId)
                                catch
                                    case e: RocksDBException =>
                                        throw e
                                    case NonFatal(ex) =>
                                        logger.error(
                                            s"Error processing domain message ($qmid) and codomain message ($codomainMessageId): ${ex.stackTraceAsString}"
                                        )
                                        None
                    (payloadsOut.distinct, passthroughs)

    private def compose(
        currentDomainMessageId: QualifiedMessageId,
        codomainMessage: ObjectNode,
        stateStore: StateStore
    ): Unit =
        val currentDomainMessageOpt: Option[JsonNode] =
            stateStore.getJson(s"${StateStoreSection.DOM}/$currentDomainMessageId")
        val currentDomainMessageMap: ObjectNode =
            Option(codomainMessage.get(currentDomainMessageId.qualifierString))
                .fold(mapper.createObjectNode())(_.asInstanceOf[ObjectNode])
        setObject(currentDomainMessageMap, currentDomainMessageId.id.toString, currentDomainMessageOpt)
        setObject(
            codomainMessage,
            currentDomainMessageId.qualifierString,
            if currentDomainMessageMap.isEmpty then None else Some(currentDomainMessageMap)
        )

        val next: Set[QualifiedMessageId] =
            stateStore
                .getStringSet(
                    s"${StateStoreSection.LNK}/$currentDomainMessageId"
                )
                .map: entry =>
                    val splittedEntry = entry.split("/")
                    QualifiedMessageId(
                        ChannelName(splittedEntry(0)),
                        MessageFormatName(splittedEntry(1)),
                        MessageId(splittedEntry(2))
                    )
        next.foreach: nextMessageId =>
            compose(nextMessageId, codomainMessage, stateStore)

    private def setObject(parent: ObjectNode, name: String, objOpt: Option[JsonNode]): Unit =
        objOpt match
            case Some(obj) =>
                parent.set[JsonNode](name, obj)
            case None =>
                parent.remove(name)
