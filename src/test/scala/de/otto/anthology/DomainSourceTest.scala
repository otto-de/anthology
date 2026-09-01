package de.otto.anthology

import com.jayway.jsonpath.JsonPath
import de.otto.anthology.DomainSource
import de.otto.anthology.JsonSupport.mapper
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
        val topic = TopicName("topic-domain-x-test1")
        val group = "group"

        publishToKafka(topic.toString, "1", """{ "id": "1", "foo": "barA" }""")
        publishToKafka(topic.toString, "2", """{ "id": "2", "foo": "barB" }""")
        publishToKafka(topic.toString, "3", """{ "id": "3", "foo": "barC" }""")

        // when
        val sourceConfig = KafkaSourceConfig(cluster, topic, group, None)

        val aggregateConfigA =
            MessageFormatConfig(
                MessageFormatName("Agg-A"),
                Some(JsonPath.compile("$[?(@.foo == 'barA')]")),
                None,
                None,
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
                None,
                None,
                None
            )

        // Non recognitionPath given - should not be recognised:
        val aggregateConfigC =
            MessageFormatConfig(MessageFormatName("Agg-C"), None, None, None, None, None, None)

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
        val topic = TopicName("topic-domain-x-test2")
        val group = "group"

        publishToKafka(topic.toString, "1", """{ "id": "1", "foo": "barA" }""")
        publishToKafka(topic.toString, "2", """{ "id": "2", "foo": "barB" }""")
        publishToKafka(topic.toString, "3", """{ "id": "3", "foo": "barC" }""")

        // when
        val sourceConfig = KafkaSourceConfig(cluster, topic, group, None)

        val aggregateConfigA =
            MessageFormatConfig(MessageFormatName("Agg-A"), None, None, None, None, None, None)

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
        val topic = TopicName("topic-domain-x-test3")
        val group = "group"

        publishToKafka(topic.toString, "1", """{ "id": "1", "foo": "barA" }""")
        publishToKafka(topic.toString, "2", """{ "id": "2", "foo": "barB" }""")
        publishToKafka(topic.toString, "3", """{ "id": "3", "foo": "barC" }""")

        // when
        val sourceConfig = KafkaSourceConfig(cluster, topic, group, None)

        val aggregateConfigA =
            MessageFormatConfig(
                MessageFormatName("Agg-A"),
                Some(JsonPath.compile("$[?(@.foo == 'barA')]")),
                None,
                None,
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

    it should "extract key from message when there is a key extraction path configured" in:
        // given
        val cluster = ClusterName("cluster-x")
        val topic = TopicName("topic-domain-x-test4")
        val group = "group"

        val message1 = """{ "id": "1", "foo": "barA" }"""
        val message2 = """{ "id": "2", "foo": "barB" }"""
        val message3 = """{ "id": "3", "foo": "barC" }"""

        publishToKafka(topic.toString, "X", message1)
        publishToKafka(topic.toString, "X", message2)
        publishToKafka(topic.toString, "X", message3)

        // when
        val sourceConfig = KafkaSourceConfig(cluster, topic, group, None)

        val aggregateConfigA =
            MessageFormatConfig(
                MessageFormatName("Agg-A"),
                None,
                Some(JsonPath.compile("$.id")), // extract id from message and use it as message id
                None,
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
                result1._1.exists(rec =>
                    rec._1 == QualifiedMessageId(
                        ChannelName("domain-x"),
                        MessageFormatName("Agg-A"),
                        MessageId("1")
                    ) && rec._2 == Some(Message(mapper.readTree(message1)))
                )
            )

            val result2: (Option[(QualifiedMessageId, Option[Message])], Passthrough) = channel.receive()
            assert(
                result2._1.exists(rec =>
                    rec._1 == QualifiedMessageId(
                        ChannelName("domain-x"),
                        MessageFormatName("Agg-A"),
                        MessageId("2")
                    ) && rec._2 == Some(Message(mapper.readTree(message2)))
                )
            )

            val result3: (Option[(QualifiedMessageId, Option[Message])], Passthrough) = channel.receive()
            assert(
                result3._1.exists(rec =>
                    rec._1 == QualifiedMessageId(
                        ChannelName("domain-x"),
                        MessageFormatName("Agg-A"),
                        MessageId("3")
                    ) && rec._2 == Some(Message(mapper.readTree(message3)))
                )
            )

    it should "skip message when message key cannot be extracted from message" in:
        // given
        val cluster = ClusterName("cluster-x")
        val topic = TopicName("topic-domain-x-test5")
        val group = "group"

        val message1 = """{ "id": "1", "foo": "barA" }"""
        val message2 = """{ "xx": "2", "foo": "barB" }"""
        val message3 = """{ "id": "3", "foo": "barC" }"""

        publishToKafka(topic.toString, "X", message1)
        publishToKafka(topic.toString, "X", message2)
        publishToKafka(topic.toString, "X", message3)

        // when
        val sourceConfig = KafkaSourceConfig(cluster, topic, group, None)

        val aggregateConfigA =
            MessageFormatConfig(
                MessageFormatName("Agg-A"),
                None,
                Some(JsonPath.compile("$.id")), // extract id from message and use it as message id
                None,
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
                result1._1.exists(rec =>
                    rec._1 == QualifiedMessageId(
                        ChannelName("domain-x"),
                        MessageFormatName("Agg-A"),
                        MessageId("1")
                    ) && rec._2 == Some(Message(mapper.readTree(message1)))
                )
            )

            val result2: (Option[(QualifiedMessageId, Option[Message])], Passthrough) = channel.receive()
            assert(
                result2._1.isEmpty
            )

            val result3: (Option[(QualifiedMessageId, Option[Message])], Passthrough) = channel.receive()
            assert(
                result3._1.exists(rec =>
                    rec._1 == QualifiedMessageId(
                        ChannelName("domain-x"),
                        MessageFormatName("Agg-A"),
                        MessageId("3")
                    ) && rec._2 == Some(Message(mapper.readTree(message3)))
                )
            )
