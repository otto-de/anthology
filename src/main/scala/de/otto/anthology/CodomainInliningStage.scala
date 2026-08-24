package de.otto.anthology

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.ChannelName
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.Message
import de.otto.anthology.MessageFormatName
import de.otto.anthology.MessageId
import de.otto.anthology.Parallelism
import de.otto.anthology.QualifiedMessageId
import de.otto.anthology.SimpleProcessingTimeLogger.measureMap
import de.otto.anthology.config.RelationConfigs
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.statestore.StateStoreSection
import de.otto.anthology.util.ExceptionUtil.stackTraceAsString
import org.rocksdb.RocksDBException
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

    extension (in: Flow[(Seq[MessageId], Seq[Passthrough])])
        def inlineDomainMessages(
            config: RelationConfigs,
            stateStore: StateStore,
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Seq[(MessageId, Option[Message])], Seq[Passthrough])] =
            in.mapPar(parallelism.toInt):
                measureMap("CodomainInlining"): (codomainMessageIds, passthroughs) =>
                    val results: Seq[(MessageId, Option[Message])] =
                        codomainMessageIds.flatMap: codomainMessageId =>
                            try
                                val codomainKeyStaged = s"${StateStoreSection.STA}/${codomainMessageId.toString}"
                                stateStore
                                    .getJson(codomainKeyStaged)
                                    .map(_.asInstanceOf[ObjectNode])
                                    .map: codomainMessageStaged =>
                                        val codomainMessageTemp: ObjectNode = mapper.createObjectNode()
                                        doInline(
                                            QualifiedMessageId(config.root._1, config.root._2, codomainMessageId),
                                            codomainMessageTemp,
                                            codomainMessageStaged,
                                            stateStore
                                        )
                                        val rootKey: String = s"${config.root._1}/${config.root._2}"
                                        val codomainMessageOpt: Option[Message] =
                                            Option(codomainMessageTemp.get(rootKey))
                                                .map(_.asInstanceOf[ArrayNode])
                                                .map(_.get(0))
                                                .map(Message(_))
                                        (codomainMessageId, codomainMessageOpt)
                            catch
                                case e: RocksDBException =>
                                    throw e
                                case NonFatal(ex) =>
                                    logger.error(
                                        s"Error processing codomain message ($codomainMessageId): ${ex.stackTraceAsString}"
                                    )
                                    None
                    (results, passthroughs)

    private def doInline(
        currentDomainMessageId: QualifiedMessageId,
        parentMessage: ObjectNode,
        codomainMessageStaged: ObjectNode,
        stateStore: StateStore
    ): Unit =
        val currentDomainMessageOpt: Option[ObjectNode] =
            Option(codomainMessageStaged.get(currentDomainMessageId.qualifierString))
                .map(_.asInstanceOf[ObjectNode])
                .flatMap(j => Option(j.get(currentDomainMessageId.id.toString)).map(_.asInstanceOf[ObjectNode]))
                .map(_.deepCopy())

        currentDomainMessageOpt.foreach: currentDomainMessage =>
            val currentDomainMessagesArray: ArrayNode =
                Option(parentMessage.get(currentDomainMessageId.qualifierString))
                    .fold {
                        val array = mapper.createArrayNode()
                        parentMessage.set(currentDomainMessageId.qualifierString, array)
                        array
                    }(_.asInstanceOf[ArrayNode])
            currentDomainMessagesArray.add(currentDomainMessage)

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
                doInline(nextMessageId, currentDomainMessage, codomainMessageStaged, stateStore)
