package de.otto.anthology

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.DomainSource
import de.otto.anthology.KafkaSourceConfig
import de.otto.anthology.KafkaSourceSettings
import de.otto.anthology.Message
import de.otto.anthology.MessageId
import de.otto.anthology.QualifiedMessageId
import de.otto.anthology.config.ChannelConfig
import de.otto.anthology.config.MessageFormatConfig
import de.otto.anthology.kafka.ClusterName
import de.otto.anthology.kafka.ConsumerName
import de.otto.anthology.kafka.MessageDeserializer
import de.otto.anthology.kafka.MessageIdDeserializer
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.kafka.TopicName
import io.github.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.*
import ox.kafka.ConsumerSettings
import ox.kafka.ConsumerSettings.AutoOffsetReset

import scala.compiletime.uninitialized

class DomainSourceTest extends AnyFlatSpec, Matchers, Diagrams, EmbeddedKafka, BeforeAndAfterAll:

    given StringSerializer = new StringSerializer

    private var bootstrapServer: String = uninitialized

    override def beforeAll(): Unit =
        bootstrapServer = s"localhost:${EmbeddedKafka.start().config.kafkaPort}"

    override def afterAll(): Unit =
        EmbeddedKafka.stop()

    "DomainSource" should "recognise different aggregate configs" in:
        // given
        val cluster = ClusterName("cluster-x")
        val topic = TopicName("topic-domain-x")
        val group = "group"

        publishToKafka(topic.toString, "1", """{ "id": "1", "foo": "barA" }""")
        publishToKafka(topic.toString, "2", """{ "id": "2", "foo": "barB" }""")
        publishToKafka(topic.toString, "3", """{ "id": "3", "foo": "barC" }""")

        // when
        val sourceConfig = KafkaSourceConfig(cluster, topic, group)

        val aggregateConfigA =
            MessageFormatConfig(
                MessageFormatName("Agg-A"),
                Some(JsonPath.compile("$[?(@.foo == 'barA')]")),
                None,
                None,
                None
            )

        val aggregateConfigB =
            MessageFormatConfig(
                MessageFormatName("Agg-B"),
                Some(JsonPath.compile("$[?(@.foo == 'barB')]")),
                None,
                None,
                None
            )

        // Non recognitionPath given - should not be recognised:
        val aggregateConfigC =
            MessageFormatConfig(MessageFormatName("Agg-C"), None, None, None, None)

        val domainConfig =
            ChannelConfig(
                ChannelName("domain-x"),
                sourceConfig,
                Seq(aggregateConfigA, aggregateConfigB, aggregateConfigC)
            )

        supervised:
            val consumerSettings: ConsumerSettings[MessageId, Option[Message]] =
                ConsumerSettings
                    .default(domainConfig.kafka.consumerGroup)
                    .bootstrapServers(bootstrapServer.split(",").map(_.trim)*)
                    .keyDeserializer(MessageIdDeserializer)
                    .valueDeserializer(MessageDeserializer)
                    .autoOffsetReset(AutoOffsetReset.Earliest)
            val consumer = consumerSettings.toThreadSafeConsumerWrapper
            val sourceSettings = KafkaSourceSettings(sourceConfig, ConsumerName(domainConfig.name.toString), consumer)
            val domainSource = DomainSource(domainConfig, sourceSettings)

            val channel = domainSource.runToChannel()

            // then
            val result1: (Option[(QualifiedMessageId, Option[Message])], Passthrough) = channel.receive()
            assert(
                result1._1.exists(
                    _._1 == QualifiedMessageId(ChannelName("domain-x"), MessageFormatName("Agg-A"), MessageId("1"))
                )
            )

            val result2: (Option[(QualifiedMessageId, Option[Message])], Passthrough) = channel.receive()
            assert(
                result2._1.exists(
                    _._1 == QualifiedMessageId(ChannelName("domain-x"), MessageFormatName("Agg-B"), MessageId("2"))
                )
            )

            val result3: (Option[(QualifiedMessageId, Option[Message])], Passthrough) = channel.receive()
            assert(
                result3._1.isEmpty
            )

    it should "skip recognition when there is only one aggregate config" in:
        // given
        val cluster = ClusterName("cluster-x")
        val topic = TopicName("topic-domain-x")
        val group = "group"

        publishToKafka(topic.toString, "1", """{ "id": "1", "foo": "barA" }""")
        publishToKafka(topic.toString, "2", """{ "id": "2", "foo": "barB" }""")
        publishToKafka(topic.toString, "3", """{ "id": "3", "foo": "barC" }""")

        // when
        val sourceConfig = KafkaSourceConfig(cluster, topic, group)

        val aggregateConfigA =
            MessageFormatConfig(MessageFormatName("Agg-A"), None, None, None, None)

        val domainConfig =
            ChannelConfig(
                ChannelName("domain-x"),
                sourceConfig,
                Seq(aggregateConfigA)
            )

        supervised:
            val consumerSettings: ConsumerSettings[MessageId, Option[Message]] =
                ConsumerSettings
                    .default(domainConfig.kafka.consumerGroup)
                    .bootstrapServers(bootstrapServer.split(",").map(_.trim)*)
                    .keyDeserializer(MessageIdDeserializer)
                    .valueDeserializer(MessageDeserializer)
                    .autoOffsetReset(AutoOffsetReset.Earliest)
            val consumer = consumerSettings.toThreadSafeConsumerWrapper
            val sourceSettings = KafkaSourceSettings(sourceConfig, ConsumerName(domainConfig.name.toString), consumer)
            val domainSource = DomainSource(domainConfig, sourceSettings)

            val channel = domainSource.runToChannel()

            // then
            val result1: (Option[(QualifiedMessageId, Option[Message])], Passthrough) = channel.receive()
            assert(
                result1._1.exists(
                    _._1 == QualifiedMessageId(ChannelName("domain-x"), MessageFormatName("Agg-A"), MessageId("1"))
                )
            )

            val result2: (Option[(QualifiedMessageId, Option[Message])], Passthrough) = channel.receive()
            assert(
                result2._1.exists(
                    _._1 == QualifiedMessageId(ChannelName("domain-x"), MessageFormatName("Agg-A"), MessageId("2"))
                )
            )

            val result3: (Option[(QualifiedMessageId, Option[Message])], Passthrough) = channel.receive()
            assert(
                result3._1.exists(
                    _._1 == QualifiedMessageId(ChannelName("domain-x"), MessageFormatName("Agg-A"), MessageId("3"))
                )
            )

    it should "recognise one aggregate config and ignore the rest" in:
        // given
        val cluster = ClusterName("cluster-x")
        val topic = TopicName("topic-domain-x")
        val group = "group"

        publishToKafka(topic.toString, "1", """{ "id": "1", "foo": "barA" }""")
        publishToKafka(topic.toString, "2", """{ "id": "2", "foo": "barB" }""")
        publishToKafka(topic.toString, "3", """{ "id": "3", "foo": "barC" }""")

        // when
        val sourceConfig = KafkaSourceConfig(cluster, topic, group)

        val aggregateConfigA =
            MessageFormatConfig(
                MessageFormatName("Agg-A"),
                Some(JsonPath.compile("$[?(@.foo == 'barA')]")),
                None,
                None,
                None
            )

        val domainConfig =
            ChannelConfig(
                ChannelName("domain-x"),
                sourceConfig,
                Seq(aggregateConfigA)
            )

        supervised:
            val consumerSettings: ConsumerSettings[MessageId, Option[Message]] =
                ConsumerSettings
                    .default(domainConfig.kafka.consumerGroup)
                    .bootstrapServers(bootstrapServer.split(",").map(_.trim)*)
                    .keyDeserializer(MessageIdDeserializer)
                    .valueDeserializer(MessageDeserializer)
                    .autoOffsetReset(AutoOffsetReset.Earliest)
            val consumer = consumerSettings.toThreadSafeConsumerWrapper
            val sourceSettings = KafkaSourceSettings(sourceConfig, ConsumerName(domainConfig.name.toString), consumer)
            val domainSource = DomainSource(domainConfig, sourceSettings)

            val channel = domainSource.runToChannel()

            // then
            val result1: (Option[(QualifiedMessageId, Option[Message])], Passthrough) = channel.receive()
            assert(
                result1._1.exists(
                    _._1 == QualifiedMessageId(ChannelName("domain-x"), MessageFormatName("Agg-A"), MessageId("1"))
                )
            )

            val result2: (Option[(QualifiedMessageId, Option[Message])], Passthrough) = channel.receive()
            assert(
                result2._1.isEmpty
            )

            val result3: (Option[(QualifiedMessageId, Option[Message])], Passthrough) = channel.receive()
            assert(
                result3._1.isEmpty
            )
