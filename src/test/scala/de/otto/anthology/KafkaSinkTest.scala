package de.otto.anthology

import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.JsonSupport.mapper
import de.otto.anthology.KafkaSink.emit
import de.otto.anthology.KafkaSinkConfig
import de.otto.anthology.KafkaSinkSettings
import de.otto.anthology.KafkaSource
import de.otto.anthology.KafkaSourceConfig
import de.otto.anthology.KafkaSourceSettings
import de.otto.anthology.config.KafkaClusterConfig
import de.otto.anthology.config.KafkaClusterSettings
import de.otto.anthology.kafka.AggregateDeserializer
import de.otto.anthology.kafka.AggregateIdDeserializer
import de.otto.anthology.kafka.ClusterName
import de.otto.anthology.kafka.ConsumerMap
import de.otto.anthology.kafka.ConsumerName
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.kafka.TopicName
import io.github.embeddedkafka.EmbeddedKafka
import io.github.embeddedkafka.EmbeddedKafkaConfig
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.*
import ox.channels.Source
import ox.flow.Flow
import ox.kafka.ConsumerSettings
import ox.kafka.ConsumerSettings.AutoOffsetReset

import scala.compiletime.uninitialized
import scala.concurrent.duration.*

class KafkaSinkTest extends AnyFlatSpec, Matchers, Diagrams, EmbeddedKafka, BeforeAndAfterAll:

    private var bootstrapServer: String = uninitialized

    given StringSerializer = new StringSerializer()
    given StringDeserializer = new StringDeserializer()

    override def beforeAll(): Unit =
        bootstrapServer = s"localhost:${EmbeddedKafka.start().config.kafkaPort}"

    override def afterAll(): Unit =
        EmbeddedKafka.stop()

    "KafkaSink" should "publish and commit messages" in:

        // given
        val cluster = ClusterName("cluster")
        val group = "group-KafkaSinkTest"

        val topicA = TopicName("topic-a")
        val topicB = TopicName("topic-b")
        val topicC = TopicName("topic-c")

        val topicT = TopicName("topic-t")

        val consumerNameA = ConsumerName(topicA.toString)
        val consumerNameB = ConsumerName(topicB.toString)
        val consumerNameC = ConsumerName(topicC.toString)

        val consumerSettings =
            ConsumerSettings
                .default(group)
                .bootstrapServers(bootstrapServer.split(",").map(_.trim)*)
                .keyDeserializer(AggregateIdDeserializer)
                .valueDeserializer(AggregateDeserializer)
                .autoOffsetReset(AutoOffsetReset.Earliest)

        val sourceConfigA = KafkaSourceConfig(cluster, topicA, group)
        val sourceConfigB = KafkaSourceConfig(cluster, topicB, group)
        val sourceConfigC = KafkaSourceConfig(cluster, topicC, group)

        val sinkConfig = KafkaSinkConfig(cluster, topicT)

        val clusterSettings = KafkaClusterSettings(KafkaClusterConfig(cluster, bootstrapServer), Map.empty)

        val messageIdA1 = "A1"
        val messageIdB1 = "B1"
        val messageIdC1 = "C1"
        val messageIdA2 = "A2"
        val messageIdB2 = "B2"
        val messageIdC2 = "C2"
        val messageIdA3 = "A3"
        val messageIdB3 = "B3"
        val messageIdC3 = "C3"

        val messageIdT1 = "T1"
        val messageIdT2 = "T2"
        val messageIdT3 = "T3"
        val messageIdT4 = "T4"
        val messageIdT5 = "T5"

        val messageA1 = """{"foo":"barA1"}"""
        val messageB1 = """{"foo":"barB1"}"""
        val messageC1 = """{"foo":"barC1"}"""
        val messageA2 = """{"foo":"barA2"}"""
        val messageB2 = """{"foo":"barB2"}"""
        val messageC2 = """{"foo":"barC2"}"""
        val messageA3 = """{"foo":"barA3"}"""
        val messageB3 = """{"foo":"barB3"}"""
        val messageC3 = """{"foo":"barC3"}"""

        val messageT1 = """{"hello":"world1"}"""
        val messageT2 = """{"hello":"world2"}"""
        val messageT3 = """{"hello":"world3"}"""
        val messageT4 = """{"hello":"world4"}"""
        val messageT5 = """{"hello":"world5"}"""

        // fill source topics
        publishToKafka(topicA.toString, messageIdA1, messageA1)
        publishToKafka(topicB.toString, messageIdB1, messageB1)
        publishToKafka(topicC.toString, messageIdC1, messageC1)
        publishToKafka(topicA.toString, messageIdA2, messageA2)
        publishToKafka(topicB.toString, messageIdB2, messageB2)
        publishToKafka(topicC.toString, messageIdC2, messageC2)
        publishToKafka(topicA.toString, messageIdA3, messageA3)
        publishToKafka(topicB.toString, messageIdB3, messageB3)
        publishToKafka(topicC.toString, messageIdC3, messageC3)

        {
            // when (1)
            supervised:
                // setup input - consuming from sources
                val consumers: ConsumerMap =
                    Map(
                        consumerNameA -> consumerSettings.toThreadSafeConsumerWrapper,
                        consumerNameB -> consumerSettings.toThreadSafeConsumerWrapper,
                        consumerNameC -> consumerSettings.toThreadSafeConsumerWrapper
                    )

                val kafkaSourceA =
                    KafkaSource(KafkaSourceSettings(sourceConfigA, consumerNameA, consumers(consumerNameA)))
                val kafkaSourceB =
                    KafkaSource(KafkaSourceSettings(sourceConfigB, consumerNameB, consumers(consumerNameB)))
                val kafkaSourceC =
                    KafkaSource(KafkaSourceSettings(sourceConfigC, consumerNameC, consumers(consumerNameC)))

                val inputChannelA: Source[Passthrough] = kafkaSourceA.runToChannel()
                val inputChannelB: Source[Passthrough] = kafkaSourceB.runToChannel()
                val inputChannelC: Source[Passthrough] = kafkaSourceC.runToChannel()

                // setup output - publishing and committing
                val payloads1 = Seq(
                    (AggregateId(messageIdT1), Some(Aggregate(mapper.readTree(messageT1))), None),
                    (AggregateId(messageIdT2), Some(Aggregate(mapper.readTree(messageT2))), None)
                )
                val payloads2 = Seq(
                    (AggregateId(messageIdT3), Some(Aggregate(mapper.readTree(messageT3))), None),
                    (AggregateId(messageIdT4), Some(Aggregate(mapper.readTree(messageT4))), None),
                    (AggregateId(messageIdT5), Some(Aggregate(mapper.readTree(messageT5))), None)
                )
                val passthroughs1 = Seq(
                    inputChannelA.receive()
                )
                val passthroughs2 = Seq(
                    inputChannelB.receive(),
                    inputChannelC.receive()
                )

                val outputFlow: Flow[(Seq[(AggregateId, Option[Aggregate], Option[Headers])], Seq[Passthrough])] =
                    Flow.fromValues(
                        (payloads1, passthroughs1),
                        (payloads2, passthroughs2)
                    )

                // run the sink
                forkDiscard:
                    outputFlow.emit(KafkaSinkSettings(sinkConfig, clusterSettings, consumers))

                // then (1) - check publishing
                // Can we find the expected messages on the target topic?
                val publishedMessages = consumeNumberKeyedMessagesFrom(topicT.toString, 5, timeout = 30.seconds).toMap
                assert(publishedMessages.contains(messageIdT1))
                assert(publishedMessages.contains(messageIdT2))
                assert(publishedMessages.contains(messageIdT3))
                assert(publishedMessages.contains(messageIdT4))
                assert(publishedMessages.contains(messageIdT5))
                assert(publishedMessages(messageIdT1) == messageT1)
                assert(publishedMessages(messageIdT2) == messageT2)
                assert(publishedMessages(messageIdT3) == messageT3)
                assert(publishedMessages(messageIdT4) == messageT4)
                assert(publishedMessages(messageIdT5) == messageT5)

                sleep(5.seconds)
        }

        {
            // then (2) - check committing
            // When we resume consuming, do we receive the remaing messages from the source topics (but not the
            // already consumed ones?)
            supervised:
                // re-setup consuming from sources
                val consumers: ConsumerMap =
                    Map(
                        consumerNameA -> consumerSettings.toThreadSafeConsumerWrapper,
                        consumerNameB -> consumerSettings.toThreadSafeConsumerWrapper,
                        consumerNameC -> consumerSettings.toThreadSafeConsumerWrapper
                    )

                val kafkaSourceA =
                    KafkaSource(KafkaSourceSettings(sourceConfigA, consumerNameA, consumers(consumerNameA)))
                val kafkaSourceB =
                    KafkaSource(KafkaSourceSettings(sourceConfigB, consumerNameB, consumers(consumerNameB)))
                val kafkaSourceC =
                    KafkaSource(KafkaSourceSettings(sourceConfigC, consumerNameC, consumers(consumerNameC)))

                val inputChannelA: Source[Passthrough] = kafkaSourceA.runToChannel()
                val inputChannelB: Source[Passthrough] = kafkaSourceB.runToChannel()
                val inputChannelC: Source[Passthrough] = kafkaSourceC.runToChannel()

                val consumedKeys = Set(
                    inputChannelA.receive()._1.key,
                    inputChannelB.receive()._1.key,
                    inputChannelC.receive()._1.key,
                    inputChannelA.receive()._1.key,
                    inputChannelB.receive()._1.key,
                    inputChannelC.receive()._1.key
                )

                val expectedKeys = Set(
                    AggregateId(messageIdA2),
                    AggregateId(messageIdB2),
                    AggregateId(messageIdC2),
                    AggregateId(messageIdA3),
                    AggregateId(messageIdB3),
                    AggregateId(messageIdC3)
                )

                assert(consumedKeys == expectedKeys)
        }
