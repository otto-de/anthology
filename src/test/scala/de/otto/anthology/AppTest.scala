package de.otto.anthology

import io.github.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.*

import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import scala.concurrent.duration.*

class AppTest extends AnyFlatSpec, Matchers, Diagrams, EmbeddedKafka, BeforeAndAfterAll:

    given StringSerializer = new StringSerializer()
    given StringDeserializer = new StringDeserializer()

    override def beforeAll(): Unit =
        s"localhost:${EmbeddedKafka.start().config.kafkaPort}"

    override def afterAll(): Unit =
        EmbeddedKafka.stop()

    "App" should "produce expected messages" in:

        val tempDir: Path = Paths.get(System.getProperty("java.io.tmpdir"))
        val configDir: Path = Paths.get(URLDecoder.decode(getClass.getResource("/e2e").getFile, StandardCharsets.UTF_8))

        val credentials = """{"test-cluster":{}}"""

        val cliArgs =
            Vector(
                "--anthology-config-file",
                s"$configDir/application.yaml",
                "--anthology-credentials",
                credentials,
                "--anthology-state-store-path",
                s"$tempDir/anthology-data"
            )

        val mediaTopic = "media"
        val authorsTopic = "authors"

        // fill source topics
        publishToKafka(mediaTopic, TestData.setupBookIdEasy.toString, TestData.setupBookEasy.toJson.toPrettyString)
        publishToKafka(
            authorsTopic,
            TestData.setupAuthorIdTolkien.toString,
            TestData.setupAuthorTolkien.toJson.toPrettyString
        )
        publishToKafka(
            authorsTopic,
            TestData.setupAuthorIdChristie.toString,
            TestData.setupAuthorCristie.toJson.toPrettyString
        )
        publishToKafka(
            authorsTopic,
            TestData.setupAuthorIdKnuth.toString,
            TestData.setupAuthorKnuth.toJson.toPrettyString
        )
        publishToKafka(
            authorsTopic,
            TestData.setupAuthorIdFeynman.toString,
            TestData.setupAuthorFeynman.toJson.toPrettyString
        )

        // run application
        supervised:
            timeoutOption(1.minute):
                App.run(cliArgs)

        // consume from target topic
        val aggregatedMessages = consumeNumberKeyedMessagesFrom(number = 1, topic = "rich-books")

        // TODO assert read messages
        println("AGGREGATED: " + aggregatedMessages.mkString(" +++ "))
        succeed
