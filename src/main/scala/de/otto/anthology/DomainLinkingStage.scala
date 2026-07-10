package de.otto.anthology

import com.fasterxml.jackson.databind.node.ValueNode
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.DocumentContext
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option as JsonPathOption
import com.jayway.jsonpath.ParseContext
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.ChannelName
import de.otto.anthology.Message
import de.otto.anthology.MessageFormatName
import de.otto.anthology.MessageId
import de.otto.anthology.Parallelism
import de.otto.anthology.QualifiedMessageId
import de.otto.anthology.config.RelationConfigs
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.statestore.StateStoreSection
import de.otto.anthology.util.ExceptionUtil.stackTraceAsString
import org.rocksdb.RocksDBException
import ox.filterPar
import ox.flow.Flow
import ox.foreachPar
import ox.mapPar

import scala.util.control.NonFatal

object DomainLinkingStage extends LazyLogging:

    private val jsonPathContext: ParseContext =
        JsonPath.using(
            Configuration
                .builder()
                .jsonProvider(JacksonJsonNodeJsonProvider())
                .options(JsonPathOption.SUPPRESS_EXCEPTIONS) // When no match: null instead of exception
                .build()
        )

    extension (in: Flow[(Option[QualifiedMessageId], Passthrough)])
        def linkDomainMessages(
            config: RelationConfigs,
            stateStore: StateStore,
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Option[QualifiedMessageId], Passthrough)] =
            in.map:
                case (None, pass) =>
                    (None, pass)

                case (Some(qmid), pass) =>
                    try
                        val cacheMap = new scala.collection.mutable.HashMap[String, Option[Array[Byte]]]
                        val cache = new StateStore:
                            def get(key: String): Option[Array[Byte]] =
                                cacheMap.getOrElseUpdate(key, stateStore.get(key))
                            def put(key: String, value: Array[Byte]): Unit = cacheMap.put(key, Some(value))
                            def delete(key: String): Unit = cacheMap.put(key, None)

                        val messageOpt: Option[Message] =
                            stateStore
                                .getJson(s"${StateStoreSection.DOM}/$qmid")
                                .map(Message(_))

                        messageOpt match
                            case None =>
                                // Domain message was deleted, so delete all associated links & backlinks
                                // when messages on both ends are deleted
                                // TODO only when "many"-side was deleted?

                                try
                                    val linkKey = s"${StateStoreSection.LNK}/$qmid"
                                    val linkValues =
                                        stateStore
                                            .getStringSet(linkKey)
                                            .filterPar(parallelism.toInt)(v =>
                                                stateStore.get(s"${StateStoreSection.DOM}/$v").isEmpty
                                            )

                                    // Delete backlinks ending here
                                    linkValues.foreachPar(parallelism.toInt): value =>
                                        val _backLinkKey = s"${StateStoreSection.BLK}/$value"
                                        stateStore.removeStringFromSet(_backLinkKey, qmid.toString)

                                    // Delete links starting here
                                    stateStore.removeStringsFromSet(linkKey, linkValues)

                                    val backLinkKey = s"${StateStoreSection.BLK}/$qmid"
                                    val backLinkValues =
                                        stateStore
                                            .getStringSet(backLinkKey)
                                            .filterPar(parallelism.toInt)(v =>
                                                stateStore.get(s"${StateStoreSection.DOM}/$v").isEmpty
                                            )

                                    // Delete links ending here
                                    backLinkValues.foreachPar(parallelism.toInt): value =>
                                        val _linkKey = s"${StateStoreSection.LNK}/$value"
                                        stateStore.removeStringFromSet(_linkKey, qmid.toString)

                                    // Delete backlinks starting here
                                    stateStore.removeStringsFromSet(backLinkKey, backLinkValues)

                                    (Some(qmid), pass)
                                catch
                                    case e: RocksDBException =>
                                        throw e
                                    case NonFatal(ex) =>
                                        logger.error(s"Error processing deletion ($qmid): ${ex.stackTraceAsString}")
                                        (None, pass)

                            case Some(message) =>
                                try
                                    val parsedDoc: DocumentContext = jsonPathContext.parse(message.toJson)

                                    // (a) compute and update many-to-one relations starting here
                                    // (a.1) links
                                    val linkKey = s"${StateStoreSection.LNK}/$qmid"

                                    val linkValuesOld: Map[(ChannelName, MessageFormatName), MessageId] =
                                        stateStore
                                            .getStringSet(linkKey)
                                            .map: entry =>
                                                val splittedEntry = entry.split("/")
                                                (
                                                    ChannelName(splittedEntry(0)),
                                                    MessageFormatName(splittedEntry(1))
                                                ) -> MessageId(splittedEntry(2))
                                            .toMap

                                    val (linkRemovalOpts: Set[Option[String]], linkAdditionOpts: Set[Option[String]]) =
                                        config.manyToOneRelationsStartingFrom
                                            .getOrElse(qmid.qualifier, Set.empty)
                                            .mapPar(parallelism.toInt): mtoConfig =>
                                                val toMessageKeyOldOpt =
                                                    linkValuesOld
                                                        .get(mtoConfig.relTo)
                                                        .map(toMessageId =>
                                                            s"${mtoConfig.relTo._1}/${mtoConfig.relTo._2}/$toMessageId"
                                                        )
                                                val toMessageKeyNewOpt =
                                                    Option(parsedDoc.read[ValueNode](mtoConfig.refFromManyToOnePath))
                                                        .map(v =>
                                                            if v.canConvertToLong then v.longValue else v.textValue
                                                        )
                                                        .map(_.toString)
                                                        .map(MessageId(_))
                                                        .map(toMessageId =>
                                                            s"${mtoConfig.relTo._1}/${mtoConfig.relTo._2}/$toMessageId"
                                                        )
                                                (toMessageKeyOldOpt, toMessageKeyNewOpt) match
                                                    case (None, Some(msgN)) =>
                                                        // add msgN
                                                        (None, Some(msgN))
                                                    case (Some(msgO), None) =>
                                                        // remove msgO
                                                        (Some(msgO), None)
                                                    case (Some(msgO), Some(msgN)) if msgO != msgN =>
                                                        // remove msgO, add msgN
                                                        (Some(msgO), Some(msgN))
                                                    case _ =>
                                                        // do nothing
                                                        (None, None)
                                            .unzip

                                    val (linkRemovals: Set[String], linkAdditions: Set[String]) =
                                        (linkRemovalOpts.flatten, linkAdditionOpts.flatten)

                                    stateStore.removeStringsFromSet(linkKey, linkRemovals)

                                    stateStore.addStringsToSet(linkKey, linkAdditions)

                                    // (a.2) back links
                                    linkAdditions.foreachPar(parallelism.toInt): value =>
                                        val _backLinkKey = s"${StateStoreSection.BLK}/$value"
                                        stateStore.addStringToSet(_backLinkKey, qmid.toString)
                                    linkRemovals.foreachPar(parallelism.toInt): value =>
                                        val _backLinkKey = s"${StateStoreSection.BLK}/$value"
                                        stateStore.removeStringFromSet(_backLinkKey, qmid.toString)

                                    // (b) compute and update one-to-many relations ending here
                                    // (b.1) back links
                                    val backLinkKey = s"${StateStoreSection.BLK}/$qmid"

                                    val backLinkValuesOld: Map[(ChannelName, MessageFormatName), MessageId] =
                                        stateStore
                                            .getStringSet(backLinkKey)
                                            .map: entry =>
                                                val splittedEntry = entry.split("/")
                                                (
                                                    ChannelName(splittedEntry(0)),
                                                    MessageFormatName(splittedEntry(1))
                                                ) -> MessageId(splittedEntry(2))
                                            .toMap

                                    val (
                                        backLinkRemovalOpts: Set[Option[String]],
                                        backLinkAdditionOpts: Set[Option[String]]
                                    ) =
                                        config.oneToManyRelationsLeadingTo
                                            .getOrElse(qmid.qualifier, Set.empty)
                                            .mapPar(parallelism.toInt): otmConfig =>
                                                val fromMessageKeyOldOpt =
                                                    backLinkValuesOld
                                                        .get(otmConfig.relFrom)
                                                        .map(fromMessageId =>
                                                            s"${otmConfig.relFrom._1}/${otmConfig.relFrom._2}/$fromMessageId"
                                                        )
                                                val fromMessageKeyNewOpt =
                                                    Option(parsedDoc.read[ValueNode](otmConfig.refFromManyToOnePath))
                                                        .map(v =>
                                                            if v.canConvertToLong then v.longValue else v.textValue
                                                        )
                                                        .map(_.toString)
                                                        .map(MessageId(_))
                                                        .map(fromMessageId =>
                                                            s"${otmConfig.relFrom._1}/${otmConfig.relFrom._2}/$fromMessageId"
                                                        )
                                                (fromMessageKeyOldOpt, fromMessageKeyNewOpt) match
                                                    case (None, Some(msgN)) =>
                                                        // add msgN
                                                        (None, Some(msgN))
                                                    case (Some(msgO), None) =>
                                                        // remove msgO
                                                        (Some(msgO), None)
                                                    case (Some(msgO), Some(msgN)) if msgO != msgN =>
                                                        // remove msgO, add msgN
                                                        (Some(msgO), Some(msgN))
                                                    case _ =>
                                                        // do nothing
                                                        (None, None)
                                            .unzip

                                    val (backLinkRemovals: Set[String], backLinkAdditions: Set[String]) =
                                        (backLinkRemovalOpts.flatten, backLinkAdditionOpts.flatten)

                                    stateStore.removeStringsFromSet(backLinkKey, backLinkRemovals)
                                    stateStore.addStringsToSet(backLinkKey, backLinkAdditions)

                                    // (b.2) links
                                    backLinkAdditions.foreachPar(parallelism.toInt): value =>
                                        val _linkKey = s"${StateStoreSection.LNK}/$value"
                                        stateStore.addStringToSet(_linkKey, qmid.toString)
                                    backLinkRemovals.foreachPar(parallelism.toInt): value =>
                                        val _linkKey = s"${StateStoreSection.LNK}/$value"
                                        stateStore.removeStringFromSet(_linkKey, qmid.toString)

                                    cacheMap
                                        .filterKeys(k =>
                                            k.startsWith(StateStoreSection.LNK.toString) || k.startsWith(
                                                StateStoreSection.BLK.toString
                                            )
                                        )
                                        .foreachEntry {
                                            // can we do batch writes here???
                                            (k, vOpt) =>
                                                case (key, Some(v)) => stateStore.put(key, v)
                                                case (key, None) => stateStore.delete(key) // value == None means delete
                                        }

                                    (Some(qmid), pass)
                                catch
                                    case e: RocksDBException =>
                                        throw e
                                    case NonFatal(ex) =>
                                        logger.error(
                                            s"Error setting links ($qmid, $message): ${ex.stackTraceAsString}"
                                        )
                                        (None, pass)

                    catch
                        case e: RocksDBException =>
                            throw e
                        case NonFatal(ex) =>
                            logger.error(
                                s"Error processing record (qualifiedId=$qmid, recordKey=${pass.record.key}, recordValue=${pass.record.value}): ${ex.stackTraceAsString}"
                            )
                            (None, pass)
