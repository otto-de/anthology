package de.otto.anthology

import de.otto.anthology.MessageId
import de.otto.anthology.QualifiedMessageId
import de.otto.anthology.SimplePerformanceMeasureStage.measure
import de.otto.anthology.kafka.Passthrough
import ox.computeIntensive
import ox.flow.Flow
import pureconfig.ConfigReader

import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map as MutableMap
import scala.concurrent.duration.*

object CodomainDeduplicationStage:

    private def defaultConfig: CodomainDeduplicationConfig = CodomainDeduplicationConfig(1000, 20.seconds)

    extension (in: Flow[(Option[(QualifiedMessageId, Set[MessageId])], Passthrough)])
        def deduplicateCodomainMessages(
            configOpt: Option[CodomainDeduplicationConfig],
            logThroughput: Option[Boolean] = None
        ): Flow[(Seq[(QualifiedMessageId, Seq[MessageId])], Seq[Passthrough])] =
            val config = configOpt.getOrElse(defaultConfig)
            if config.batchSize == 1 then
                in.map: (payload, passthrough) =>
                    (Instant.now(), (payload.toSeq.map(e => (e._1, e._2.toSeq)), Seq(passthrough)))
                .measure("CodomainDeduplication", logThroughput)
            else
                in
                    .groupedWithin(config.batchSize, config.batchingDuration)
                    .map: batches =>
                        val startingTime = Instant.now()
                        val result =
                            computeIntensive:
                                val passthroughs: ListBuffer[Passthrough] = ListBuffer.empty
                                val deduplicationMap: MutableMap[QualifiedMessageId, Set[MessageId]] =
                                    MutableMap.empty
                                batches.foreach: batch =>
                                    batch._1 match
                                        case None =>
                                            ()
                                        case Some(domainMessageId, codomainMessageIds) =>
                                            deduplicationMap.updateWith(domainMessageId):
                                                case None => Some(Set.empty ++ codomainMessageIds)
                                                case Some(curCodomainMessageIds) =>
                                                    Some(curCodomainMessageIds ++ codomainMessageIds)
                                    passthroughs += batch._2
                                (deduplicationMap.map((k, v) => (k, v.toSeq)).toSeq, passthroughs.sorted.toSeq)
                        (startingTime, result)
                    .measure("CodomainDeduplication", logThroughput)

case class CodomainDeduplicationConfig(batchSize: Int, batchingDuration: FiniteDuration) derives ConfigReader
