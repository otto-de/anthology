package de.otto.anthology

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.TextNode
import de.otto.anthology.KafkaSource
import de.otto.anthology.KafkaSourceConfig
import de.otto.anthology.KafkaSourceSettings
import de.otto.anthology.kafka.AggregateDeserializer
import de.otto.anthology.kafka.AggregateIdDeserializer
import de.otto.anthology.kafka.ClusterName
import de.otto.anthology.kafka.ConsumerName
import de.otto.anthology.kafka.TopicName
import io.github.embeddedkafka.EmbeddedKafka
import io.github.embeddedkafka.EmbeddedKafkaConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.diagrams.Diagrams
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import ox.*
import ox.kafka.ConsumerSettings
import ox.kafka.ConsumerSettings.AutoOffsetReset

import java.util.Collections.singletonMap
import scala.compiletime.uninitialized

class KafkaSourceTest extends AnyFlatSpec, Matchers, Diagrams, EmbeddedKafka, BeforeAndAfterAll:

    private var bootstrapServer: String = uninitialized

    override def beforeAll(): Unit =
        bootstrapServer = s"localhost:${EmbeddedKafka.start().config.kafkaPort}"

    override def afterAll(): Unit =
        EmbeddedKafka.stop()

    "KafkaSource" should "emit messages given a consumer" in:
        // given
        val cluster = ClusterName("cluster-x")
        val topic = TopicName("topic-a")
        val group = "group"

        val consumerSettings =
            ConsumerSettings
                .default(group)
                .bootstrapServers(bootstrapServer.split(",").map(_.trim)*)
                .keyDeserializer(AggregateIdDeserializer)
                .valueDeserializer(AggregateDeserializer)
                .autoOffsetReset(AutoOffsetReset.Earliest)

        supervised:
            val sourceConfig = KafkaSourceConfig(cluster, topic, group)
            val consumerName = ConsumerName("consumer-a")
            val consumer = consumerSettings.toThreadSafeConsumerWrapper
            val sourceSettings = KafkaSourceSettings(sourceConfig, consumerName, consumer)

            publishStringMessageToKafka(topic.toString, """{ "foo": "bar1" }""")
            publishStringMessageToKafka(topic.toString, """{ "foo": "bar2" }""")
            publishStringMessageToKafka(topic.toString, """{ "foo": "bar3" }""")
            publishStringMessageToKafka(topic.toString, "")

            // when
            val source = KafkaSource(sourceSettings)

            val channel = source.runToChannel()

            // then
            val nf = JsonNodeFactory.instance

            val result1 = channel.receive().record.value
            assert(result1.contains(ObjectNode(nf, singletonMap("foo", TextNode("bar1")))))

            val result2 = channel.receive().record.value
            assert(result2.contains(ObjectNode(nf, singletonMap("foo", TextNode("bar2")))))

            val result3 = channel.receive().record.value
            assert(result3.contains(ObjectNode(nf, singletonMap("foo", TextNode("bar3")))))

            val result4 = channel.receive().record.value
            assert(result4.isEmpty)
