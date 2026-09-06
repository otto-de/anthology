package de.otto.capella

import com.jayway.jsonpath.JsonPath
import com.typesafe.scalalogging.LazyLogging
import de.otto.capella.DomainLinkingStage.linkDomainMessages
import de.otto.capella.DomainPersistenceStage.persistDomainMessages
import de.otto.capella.JsonSupport.mapper
import de.otto.capella.config.ManyToOne
import de.otto.capella.config.RelationConfigs
import de.otto.capella.kafka.Passthrough
import de.otto.capella.statestore.RocksDBConfig
import de.otto.capella.statestore.RocksDBStateStore
import de.otto.capella.statestore.StateStore
import ox.Ox
import ox.OxApp
import ox.channels.Channel
import ox.flow.Flow
import ox.par
import ox.timeoutOption
import ox.useInScope

import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.duration.*
import scala.util.Random

/** A benchmark that simulates a complex scenario and which revealed that extremely asymmetrical relations cause
  * performance issues. In this case, it was the CW/MA-->CZ/MF relation that lead to ever-increasing execution times in
  * the DomainLinkingStage. It can now be circumvented by setting the 'omitTriggerCodomain' option.
  */
object InboundPersistenceBenchmark extends OxApp.Simple, LazyLogging:

    val rand: Random = Random()

    val dataDir: String = s"${Paths.get(System.getProperty("java.io.tmpdir"))}/capella-data/${UUID.randomUUID}"

    override def run(using Ox): Unit =

        SimpleProcessingTimeLogger.reportEveryNMessages = 1000
        SimpleProcessingTimeLogger.reportToFile = Some(Paths.get(s"$dataDir/measurements.csv"))

        val relationConfigs: RelationConfigs =
            RelationConfigs(
                Seq(
                    ManyToOne(
                        relFrom = (ChannelName("CW"), MessageFormatName("MA")),
                        relTo = (ChannelName("CX"), MessageFormatName("MB")),
                        refFromManyToOnePath = JsonPath.compile("$.id_b")
                    ),
                    // The following relation might cause performance issues, because the CZ/MF entities hold a very
                    // high number of backlinks to CW/MA entities:
                    ManyToOne(
                        relFrom = (ChannelName("CW"), MessageFormatName("MA")),
                        relTo = (ChannelName("CZ"), MessageFormatName("MF")),
                        refFromManyToOnePath = JsonPath.compile("$.id_f"),
                        // omitting trigger the codomain computation solves the performance issues here:
                        omitTriggerCodomain = true
                    ),
                    ManyToOne(
                        relFrom = (ChannelName("CX"), MessageFormatName("MB")),
                        relTo = (ChannelName("CY"), MessageFormatName("ME")),
                        refFromManyToOnePath = JsonPath.compile("$.id_e")
                    ),
                    ManyToOne(
                        relFrom = (ChannelName("CX"), MessageFormatName("MB")),
                        relTo = (ChannelName("CX"), MessageFormatName("MC")),
                        refFromManyToOnePath = JsonPath.compile("$.id_c")
                    ),
                    ManyToOne(
                        relFrom = (ChannelName("CX"), MessageFormatName("MC")),
                        relTo = (ChannelName("CX"), MessageFormatName("MD")),
                        refFromManyToOnePath = JsonPath.compile("$.id_d")
                    )
                )
            )

        val stateStore: StateStore =
            useInScope(
                RocksDBStateStore(RocksDBConfig(), s"$dataDir/rocksdb")
            )(
                _.shutdown()
            )

        val src: Channel[(Option[(QualifiedMessageId, Option[Message])], Passthrough)] =
            Channel.bufferedDefault

        val countMax_a: Int = 1000_000
        val countMax_b: Int = 300_000
        val countMax_c: Int = 100_000
        val countMax_d: Int = 500
        val countMax_e: Int = 100_000
        val countMax_f: Int = 3

        var countCur_a: Int = 0
        var countCur_b: Int = 0
        var countCur_c: Int = 0
        var countCur_d: Int = 0
        var countCur_e: Int = 0
        var countCur_f: Int = 0

        def generateData: Unit =
            while //
                countCur_a < countMax_a ||
                countCur_b < countMax_b ||
                countCur_c < countMax_c ||
                countCur_d < countMax_d ||
                countCur_e < countMax_e ||
                countCur_f < countMax_f
            do
                rand.nextInt(6) match
                    case 0 =>
                        if countCur_a < countMax_a then
                            val id = countCur_a
                            val id_b = rand.nextInt(countMax_b)
                            val id_f = rand.nextInt(countMax_f)
                            val msg = Message(mapper.readTree(s"""{ "id": $id, "id_b": $id_b, "id_f": $id_f }"""))
                            val rec = (
                                Some((QualifiedMessageId(s"CW/MA/$id"), Some(msg))),
                                TestUtils.mockedKafkaRecord(id.toString, msg.toJson)
                            )
                            src.send(rec)
                            countCur_a = countCur_a + 1
                    case 1 =>
                        if countCur_b < countMax_b then
                            val id = countCur_b
                            val id_e = rand.nextInt(countMax_e)
                            val id_c = rand.nextInt(countMax_c)
                            val msg = Message(mapper.readTree(s"""{ "id": $id, "id_e": $id_e, "id_c": $id_c }"""))
                            val rec = (
                                Some((QualifiedMessageId(s"CX/MB/$id"), Some(msg))),
                                TestUtils.mockedKafkaRecord(id.toString, msg.toJson)
                            )
                            src.send(rec)
                            countCur_b = countCur_b + 1
                    case 2 =>
                        if countCur_c < countMax_c then
                            val id = countCur_c
                            val id_d = rand.nextInt(countMax_d)
                            val msg = Message(mapper.readTree(s"""{ "id": $id, "id_d": $id_d }"""))
                            val rec = (
                                Some((QualifiedMessageId(s"CX/MC/$id"), Some(msg))),
                                TestUtils.mockedKafkaRecord(id.toString, msg.toJson)
                            )
                            src.send(rec)
                            countCur_c = countCur_c + 1
                    case 3 =>
                        if countCur_d < countMax_d then
                            val id = countCur_d
                            val msg = Message(mapper.readTree(s"""{ "id": $id }"""))
                            val rec = (
                                Some((QualifiedMessageId(s"CX/MD/$id"), Some(msg))),
                                TestUtils.mockedKafkaRecord(id.toString, msg.toJson)
                            )
                            src.send(rec)
                            countCur_d = countCur_d + 1
                    case 4 =>
                        if countCur_e < countMax_e then
                            val id = countCur_e
                            val msg = Message(mapper.readTree(s"""{ "id": $id }"""))
                            val rec = (
                                Some((QualifiedMessageId(s"CY/ME/$id"), Some(msg))),
                                TestUtils.mockedKafkaRecord(id.toString, msg.toJson)
                            )
                            src.send(rec)
                            countCur_e = countCur_e + 1
                    case 5 =>
                        if countCur_f < countMax_f then
                            val id = countCur_f
                            val msg = Message(mapper.readTree(s"""{ "id": $id }"""))
                            val rec = (
                                Some((QualifiedMessageId(s"CZ/MF/$id"), Some(msg))),
                                TestUtils.mockedKafkaRecord(id.toString, msg.toJson)
                            )
                            src.send(rec)
                            countCur_f = countCur_f + 1
                    case _ =>
                        throw new IllegalStateException()

        def executeBenchmark: Unit =
            par(
                generateData,
                timeoutOption(5.minutes)(
                    Flow.fromSource(src)
                        .persistDomainMessages(stateStore)
                        .linkDomainMessages(relationConfigs, stateStore)
                        .runDrain()
                )
            )

        // run1 - inserts
        logger.info("Starting run 1")
        executeBenchmark
        logger.info("Finished run 1")

        // run2 - updates
        logger.info("Starting run 2")
        countCur_a = 0
        countCur_b = 0
        countCur_c = 0
        countCur_d = 0
        countCur_e = 0
        countCur_f = 0

        executeBenchmark
        logger.info("Finished run 2")
