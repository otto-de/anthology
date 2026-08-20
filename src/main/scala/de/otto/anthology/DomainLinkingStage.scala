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
import de.otto.anthology.QualifiedMessageId
import de.otto.anthology.SimplePerformanceMeasureStage.measure
import de.otto.anthology.config.RelationConfigs
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.statestore.StateStore.BatchOperation
import de.otto.anthology.statestore.StateStoreSection
import de.otto.anthology.util.ExceptionUtil.stackTraceAsString
import org.rocksdb.RocksDBException
import ox.flow.Flow

import java.time.Instant
import java.util.Arrays
import scala.collection.MapView
import scala.collection.mutable.HashMap as MutableHashMap
import scala.util.control.NonFatal

object DomainLinkingStage extends LazyLogging:

    extension (in: Flow[(Option[QualifiedMessageId], Passthrough)])
        def linkDomainMessages(
            config: RelationConfigs,
            stateStore: StateStore,
            logThroughput: Option[Boolean] = None
        ): Flow[(Option[QualifiedMessageId], Passthrough)] =
            in
                .buffer()
                .map:
                    case (None, pass) =>
                        (Instant.now(), (None, pass))

                    case (Some(qmid), pass) =>
                        val startingTime = Instant.now()
                        try
                            val cache = StateStoreCache(stateStore.get)

                            val messageOpt: Option[Message] =
                                cache
                                    .getJson(s"${StateStoreSection.DOM}/$qmid")
                                    .map(Message(_))

                            messageOpt match
                                case None =>
                                    // Domain message was deleted, so delete all associated links & backlinks
                                    // when messages on both ends are deleted
                                    // TODO only when "many"-side was deleted?

                                    val linkKey = s"${StateStoreSection.LNK}/$qmid"
                                    val linkValues =
                                        cache
                                            .getStringSet(linkKey)
                                            .filter(v => cache.get(s"${StateStoreSection.DOM}/$v").isEmpty)

                                    // Delete backlinks ending here
                                    linkValues.foreach: value =>
                                        val _backLinkKey = s"${StateStoreSection.BLK}/$value"
                                        cache.removeStringFromSet(_backLinkKey, qmid.toString)

                                    // Delete links starting here
                                    cache.removeStringsFromSet(linkKey, linkValues)

                                    val backLinkKey = s"${StateStoreSection.BLK}/$qmid"
                                    val backLinkValues =
                                        cache
                                            .getStringSet(backLinkKey)
                                            .filter(v => cache.get(s"${StateStoreSection.DOM}/$v").isEmpty)

                                    // Delete links ending here
                                    backLinkValues.foreach: value =>
                                        val _linkKey = s"${StateStoreSection.LNK}/$value"
                                        cache.removeStringFromSet(_linkKey, qmid.toString)

                                    // Delete backlinks starting here
                                    cache.removeStringsFromSet(backLinkKey, backLinkValues)

                                    flushCache(cache.viewChanged, stateStore)
                                    (startingTime, (Some(qmid), pass))

                                case Some(message) =>
                                    val parsedDoc: DocumentContext = jsonPathContext.parse(message.toJson)

                                    // (a) compute and update many-to-one relations starting here
                                    // (a.1) links
                                    val linkKey = s"${StateStoreSection.LNK}/$qmid"

                                    val linkValuesOld: Map[(ChannelName, MessageFormatName), MessageId] =
                                        cache
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
                                            .map: mtoConfig =>
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

                                    cache.removeStringsFromSet(linkKey, linkRemovals)

                                    cache.addStringsToSet(linkKey, linkAdditions)

                                    // (a.2) back links
                                    linkAdditions.foreach: value =>
                                        val _backLinkKey = s"${StateStoreSection.BLK}/$value"
                                        cache.addStringToSet(_backLinkKey, qmid.toString)
                                    linkRemovals.foreach: value =>
                                        val _backLinkKey = s"${StateStoreSection.BLK}/$value"
                                        cache.removeStringFromSet(_backLinkKey, qmid.toString)

                                    // (b) compute and update one-to-many relations ending here
                                    // (b.1) back links
                                    val backLinkKey = s"${StateStoreSection.BLK}/$qmid"

                                    val backLinkValuesOld: Map[(ChannelName, MessageFormatName), MessageId] =
                                        cache
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
                                            .map: otmConfig =>
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

                                    cache.removeStringsFromSet(backLinkKey, backLinkRemovals)
                                    cache.addStringsToSet(backLinkKey, backLinkAdditions)

                                    // (b.2) links
                                    backLinkAdditions.foreach: value =>
                                        val _linkKey = s"${StateStoreSection.LNK}/$value"
                                        cache.addStringToSet(_linkKey, qmid.toString)
                                    backLinkRemovals.foreach: value =>
                                        val _linkKey = s"${StateStoreSection.LNK}/$value"
                                        cache.removeStringFromSet(_linkKey, qmid.toString)

                                    flushCache(cache.viewChanged, stateStore)
                                    (startingTime, (Some(qmid), pass))

                        catch
                            case e: RocksDBException =>
                                throw e
                            case NonFatal(ex) =>
                                logger.error(
                                    s"Error processing record (qualifiedId=$qmid, recordKey=${pass.record.key}, recordValue=${pass.record.value}): ${ex.stackTraceAsString}"
                                )
                                (startingTime, (None, pass))
            .measure("DomainLinking", logThroughput)

    private val jsonPathContext: ParseContext =
        JsonPath.using(
            Configuration
                .builder()
                .jsonProvider(JacksonJsonNodeJsonProvider())
                .options(JsonPathOption.SUPPRESS_EXCEPTIONS) // When no match: null instead of exception
                .build()
        )

    private def flushCache(
        cacheView: MapView[String, Option[Array[Byte]]],
        stateStore: StateStore
    ): Unit =
        val batch: Seq[BatchOperation] =
            cacheView
                .map:
                    case (key, Some(v)) => BatchOperation.Put(key, v)
                    case (key, None) => BatchOperation.Delete(key)
                .toSeq
        stateStore.writeBatch(batch)

    private case class StateStoreCache(getFromDb: String => Option[Array[Byte]]) extends StateStore:

        private val cacheMapInitL: MutableHashMap[String, Option[Array[Byte]]] = MutableHashMap.empty

        private val cacheMapL: MutableHashMap[String, Option[Array[Byte]]] = MutableHashMap.empty

        private val cacheMap: MutableHashMap[String, Option[Array[Byte]]] = MutableHashMap.empty

        private def loadL(key: String): Option[Array[Byte]] =
            val value = getFromDb(key)
            cacheMapInitL.put(key, value)
            value

        def get(key: String): Option[Array[Byte]] =
            if key.startsWith(StateStoreSection.LNK.toString) || key.startsWith(
                    StateStoreSection.BLK.toString
                )
            then cacheMapL.getOrElseUpdate(key, loadL(key))
            else cacheMap.getOrElseUpdate(key, getFromDb(key))

        def put(key: String, value: Array[Byte]): Unit =
            assert(
                key.startsWith(StateStoreSection.LNK.toString) || key.startsWith(
                    StateStoreSection.BLK.toString
                )
            )
            cacheMapL.put(key, Some(value))

        def delete(key: String): Unit =
            assert(
                key.startsWith(StateStoreSection.LNK.toString) || key.startsWith(
                    StateStoreSection.BLK.toString
                )
            )
            cacheMapL.put(key, None)

        def viewChanged: MapView[String, Option[Array[Byte]]] =
            cacheMapL.view.filter: k2v =>
                val (k, vOpt) = k2v
                val vInitOpt = cacheMapInitL.get(k).flatten
                (vInitOpt, vOpt) match
                    case (None, Some(_)) | (Some(_), None) =>
                        true
                    case (Some(vInit), Some(v)) if !Arrays.equals(vInit, v) =>
                        true
                    case _ =>
                        false

    end StateStoreCache

end DomainLinkingStage
