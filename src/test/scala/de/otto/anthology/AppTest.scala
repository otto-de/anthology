package de.otto.anthology

import com.fasterxml.jackson.databind.node.ObjectNode
import de.otto.anthology.JsonSupport.mapper
import io.github.embeddedkafka.EmbeddedKafka
import org.apache.commons.io.FileUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.*

import java.io.File
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.duration.*

/** End-to-end test that protects against regressions and serves as a basis for reproducing errors.
  */
class AppTest extends AnyFlatSpec, Matchers, Diagrams, EmbeddedKafka, BeforeAndAfterAll:

    given StringSerializer = new StringSerializer()
    given StringDeserializer = new StringDeserializer()

    override def beforeAll(): Unit = EmbeddedKafka.start()

    override def afterAll(): Unit = EmbeddedKafka.stop()

    /** This test ensures minimal application functionality.
      */
    "App" should "aggregate messages from two source topics with minimal configuration" in:

        val tempDir: Path = Paths.get(System.getProperty("java.io.tmpdir"))
        val configDir: Path = Paths.get(URLDecoder.decode(getClass.getResource("/e2e").getFile, StandardCharsets.UTF_8))

        val additionalProps = """{"test-cluster":{}}"""

        val cliArgs =
            Vector(
                "--anthology-config-file",
                s"$configDir/application-minimal.yaml",
                "--anthology-additional-kafka-properties",
                additionalProps,
                "--anthology-state-store-path",
                s"$tempDir/anthology-data/${UUID.randomUUID}"
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
            timeoutOption(10.seconds):
                App.run(cliArgs)

        // consume from target topic
        val aggregatedMessages = consumeNumberKeyedMessagesFrom(number = 1, topic = "rich-books")

        val expectedMessageKey = TestData.setupBookIdEasy.toString
        val expectedMessageValue = {
            val authors = {
                val authorsArray = mapper.createArrayNode()
                authorsArray.add(TestData.setupAuthorFeynman.toJson)
            }
            val book = TestData.setupBookEasy.toJson
            book
                .asInstanceOf[ObjectNode]
                .set(s"${TestData.authorsChannel}/${TestData.authorMessageFormat}", authors)
            book.toString()
        }

        // assert output
        aggregatedMessages(0)._1 shouldEqual expectedMessageKey
        aggregatedMessages(0)._2 shouldEqual expectedMessageValue

    it should "shut down on database error immediately" in:

        val tempDir: Path = Paths.get(System.getProperty("java.io.tmpdir"))
        val configDir: Path = Paths.get(URLDecoder.decode(getClass.getResource("/e2e").getFile, StandardCharsets.UTF_8))

        val additionalProps = """{"test-cluster":{}}"""

        val dataDir = tempDir.resolve(s"anthology-data/${UUID.randomUUID}")
        FileUtils.copyDirectory(
            new File(configDir.resolve("corrupted-db-data").toString),
            new File(dataDir.toString)
        )

        val cliArgs =
            Vector(
                "--anthology-config-file",
                s"$configDir/application-minimal.yaml",
                "--anthology-additional-kafka-properties",
                additionalProps,
                "--anthology-state-store-path",
                dataDir.toString
            )

        // run application
        val exitCode =
            supervised:
                App.run(cliArgs)

        assert(exitCode == ExitCode.Failure(10))
