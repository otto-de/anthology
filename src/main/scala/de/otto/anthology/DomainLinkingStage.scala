package de.otto.anthology

import com.fasterxml.jackson.databind.node.TextNode
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.DocumentContext
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option as JsonPathOption
import com.jayway.jsonpath.ParseContext
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.AggregateName
import de.otto.anthology.DomainName
import de.otto.anthology.Parallelism
import de.otto.anthology.QualifiedAggregateId
import de.otto.anthology.config.DomainRelationConfigs
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.statestore.StateStore
import de.otto.anthology.statestore.StateStoreSection
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

    extension (in: Flow[(Option[QualifiedAggregateId], Passthrough)])
        def linkDomainAggregates(
            config: DomainRelationConfigs,
            stateStore: StateStore,
            parallelism: Parallelism = Parallelism(1)
        ): Flow[(Option[QualifiedAggregateId], Passthrough)] =
            in.map:
                case (None, pass) =>
                    (None, pass)

                case (Some(qaid), pass) =>
                    try
                        val aggregateOpt: Option[Aggregate] =
                            stateStore
                                .getJson(s"${StateStoreSection.DOM}/$qaid")
                                .map(Aggregate(_))

                        aggregateOpt match
                            case None =>
                                // Domain aggregate was deleted, so delete all associated links & backlinks
                                // when aggregates on both ends are deleted
                                // TODO only when "many"-side was deleted?

                                val linkKey = s"${StateStoreSection.LNK}/$qaid"
                                val linkValues =
                                    stateStore
                                        .getStringSet(linkKey)
                                        .filterPar(parallelism.toInt)(v =>
                                            stateStore.get(s"${StateStoreSection.DOM}/$v").isEmpty
                                        )

                                // Delete backlinks ending here
                                linkValues.foreachPar(parallelism.toInt): value =>
                                    val _backLinkKey = s"${StateStoreSection.BLK}/$value"
                                    stateStore.removeStringFromSet(_backLinkKey, qaid.toString)

                                // Delete links starting here
                                stateStore.removeStringsFromSet(linkKey, linkValues)

                                val backLinkKey = s"${StateStoreSection.BLK}/$qaid"
                                val backLinkValues =
                                    stateStore
                                        .getStringSet(backLinkKey)
                                        .filterPar(parallelism.toInt)(v =>
                                            stateStore.get(s"${StateStoreSection.DOM}/$v").isEmpty
                                        )

                                // Delete links ending here
                                backLinkValues.foreachPar(parallelism.toInt): value =>
                                    val _linkKey = s"${StateStoreSection.LNK}/$value"
                                    stateStore.removeStringFromSet(_linkKey, qaid.toString)

                                // Delete backlinks starting here
                                stateStore.removeStringsFromSet(backLinkKey, backLinkValues)

                                (Some(qaid), pass)

                            case Some(aggregate) =>

                                val parsedDoc: DocumentContext = jsonPathContext.parse(aggregate.toJson)

                                // (a) compute and update many-to-one relations starting here
                                // (a.1) links
                                val linkKey = s"${StateStoreSection.LNK}/$qaid"

                                val linkValuesOld: Map[(DomainName, AggregateName), AggregateId] =
                                    stateStore
                                        .getStringSet(linkKey)
                                        .map: entry =>
                                            val splittedEntry = entry.split("/")
                                            (
                                                DomainName(splittedEntry(0)),
                                                AggregateName(splittedEntry(1))
                                            ) -> AggregateId(splittedEntry(2))
                                        .toMap

                                val (linkRemovalOpts: Set[Option[String]], linkAdditionOpts: Set[Option[String]]) =
                                    config.manyToOneRelationsStartingFrom
                                        .getOrElse(qaid.qualifier, Set.empty)
                                        .mapPar(parallelism.toInt): mtoConfig =>
                                            val toAggregateKeyOldOpt =
                                                linkValuesOld
                                                    .get(mtoConfig.to)
                                                    .map(toAggregateId =>
                                                        s"${mtoConfig.to._1}/${mtoConfig.to._2}/$toAggregateId"
                                                    )
                                            val toAggregateKeyNewOpt =
                                                Option(parsedDoc.read[TextNode](mtoConfig.toAggregatePath))
                                                    .map(_.asText)
                                                    .map(AggregateId(_))
                                                    .map(toAggregateId =>
                                                        s"${mtoConfig.to._1}/${mtoConfig.to._2}/$toAggregateId"
                                                    )
                                            (toAggregateKeyOldOpt, toAggregateKeyNewOpt) match
                                                case (None, Some(aggN)) =>
                                                    // add aggN
                                                    (None, Some(aggN))
                                                case (Some(aggO), None) =>
                                                    // remove aggO
                                                    (Some(aggO), None)
                                                case (Some(aggO), Some(aggN)) if aggO != aggN =>
                                                    // remove aggO, add aggN
                                                    (Some(aggO), Some(aggN))
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
                                    stateStore.addStringToSet(_backLinkKey, qaid.toString)
                                linkRemovals.foreachPar(parallelism.toInt): value =>
                                    val _backLinkKey = s"${StateStoreSection.BLK}/$value"
                                    stateStore.removeStringFromSet(_backLinkKey, qaid.toString)

                                // (b) compute and update one-to-many relations ending here
                                // (b.1) back links
                                val backLinkKey = s"${StateStoreSection.BLK}/$qaid"

                                val backLinkValuesOld: Map[(DomainName, AggregateName), AggregateId] =
                                    stateStore
                                        .getStringSet(backLinkKey)
                                        .map: entry =>
                                            val splittedEntry = entry.split("/")
                                            (
                                                DomainName(splittedEntry(0)),
                                                AggregateName(splittedEntry(1))
                                            ) -> AggregateId(splittedEntry(2))
                                        .toMap

                                val (
                                    backLinkRemovalOpts: Set[Option[String]],
                                    backLinkAdditionOpts: Set[Option[String]]
                                ) =
                                    config.oneToManyRelationsLeadingTo
                                        .getOrElse(qaid.qualifier, Set.empty)
                                        .mapPar(parallelism.toInt): otmConfig =>
                                            val fromAggregateKeyOldOpt =
                                                backLinkValuesOld
                                                    .get(otmConfig.from)
                                                    .map(fromAggregateId =>
                                                        s"${otmConfig.from._1}/${otmConfig.from._2}/$fromAggregateId"
                                                    )
                                            val fromAggregateKeyNewOpt =
                                                Option(parsedDoc.read[TextNode](otmConfig.fromAggregatePath))
                                                    .map(_.asText)
                                                    .map(AggregateId(_))
                                                    .map(fromAggregateId =>
                                                        s"${otmConfig.from._1}/${otmConfig.from._2}/$fromAggregateId"
                                                    )
                                            (fromAggregateKeyOldOpt, fromAggregateKeyNewOpt) match
                                                case (None, Some(aggN)) =>
                                                    // add aggN
                                                    (None, Some(aggN))
                                                case (Some(aggO), None) =>
                                                    // remove aggO
                                                    (Some(aggO), None)
                                                case (Some(aggO), Some(aggN)) if aggO != aggN =>
                                                    // remove aggO, add aggN
                                                    (Some(aggO), Some(aggN))
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
                                    stateStore.addStringToSet(_linkKey, qaid.toString)
                                backLinkRemovals.foreachPar(parallelism.toInt): value =>
                                    val _linkKey = s"${StateStoreSection.LNK}/$value"
                                    stateStore.removeStringFromSet(_linkKey, qaid.toString)

                                (Some(qaid), pass)
                    catch
                        case NonFatal(ex) =>
                            logger.error(
                                s"Error processing record (${pass.record.key}, ${pass.record.value}): ${ex.getMessage}"
                            )
                            (None, pass)
