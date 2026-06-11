package de.otto.anthology

import de.otto.anthology.AggregateId
import de.otto.anthology.QualifiedAggregateId
import de.otto.anthology.kafka.Passthrough
import ox.flow.Flow
import pureconfig.ConfigReader

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map as MutableMap
import scala.concurrent.duration.*

object CodomainDeduplicationStage:

    private def defaultConfig: CodomainDeduplicationConfig = CodomainDeduplicationConfig(1000, 20.seconds)

    extension (in: Flow[(Option[(QualifiedAggregateId, Set[AggregateId])], Passthrough)])
        def deduplicateCodomainAggregates(
            configOpt: Option[CodomainDeduplicationConfig]
        ): Flow[(Seq[(QualifiedAggregateId, Seq[AggregateId])], Seq[Passthrough])] =
            val config = configOpt.getOrElse(defaultConfig)
            in
                .groupedWithin(config.batchSize, config.batchingDuration)
                .map: batches =>
                    val passthroughs: ListBuffer[Passthrough] = ListBuffer.empty
                    val deduplicationMap: MutableMap[QualifiedAggregateId, Set[AggregateId]] = MutableMap.empty
                    batches.foreach: batch =>
                        batch._1 match
                            case None =>
                                ()
                            case Some(domainAggregateId, codomainAggregateIds) =>
                                deduplicationMap.updateWith(domainAggregateId):
                                    case None => Some(Set.empty ++ codomainAggregateIds)
                                    case Some(curCodomainAggregateIds) =>
                                        Some(curCodomainAggregateIds ++ codomainAggregateIds)
                        passthroughs += batch._2
                    (deduplicationMap.map((k, v) => (k, v.toSeq)).toSeq, passthroughs.sorted.toSeq)

case class CodomainDeduplicationConfig(batchSize: Int, batchingDuration: FiniteDuration) derives ConfigReader
