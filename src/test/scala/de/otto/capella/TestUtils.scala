package de.otto.capella

import com.fasterxml.jackson.databind.JsonNode
import de.otto.capella.KafkaSourceConfig
import de.otto.capella.Message
import de.otto.capella.MessageId
import de.otto.capella.kafka.ClusterName
import de.otto.capella.kafka.ConsumerName
import de.otto.capella.kafka.Passthrough
import de.otto.capella.kafka.TopicName
import de.otto.capella.statestore.StateStore
import org.apache.kafka.clients.consumer.ConsumerRecord
import ox.kafka.ReceivedMessage

import scala.collection.mutable

object TestUtils:

    def emptyKafkaConfig: KafkaSourceConfig = KafkaSourceConfig(ClusterName(""), TopicName(""), "", None)

    def mockedKafkaRecord(
        id: String,
        jsonNode: JsonNode,
        topic: String = "test-topic",
        partition: Int = 0,
        offset: Long = 0
    ): Passthrough =
        val record =
            new ConsumerRecord[MessageId, Option[Message]](
                topic,
                partition,
                offset,
                MessageId(id),
                Some(Message(jsonNode))
            )
        Passthrough(ReceivedMessage(record), ConsumerName("test-consumer"))

    def mockedEmptyKafkaRecord(
        id: String,
        topic: String = "test-topic",
        partition: Int = 0,
        offset: Long = 0
    ): Passthrough =
        val record =
            new ConsumerRecord[MessageId, Option[Message]](
                topic,
                partition,
                offset,
                MessageId(id),
                None
            )
        Passthrough(ReceivedMessage(record), ConsumerName("test-consumer"))

    case class InMemoryStateStore(store: mutable.Map[String, Array[Byte]]) extends StateStore:

        override def get(id: String): Option[Array[Byte]] = store.get(id)

        override def put(id: String, value: Array[Byte]): Unit =
            store.update(id, value)

        override def delete(id: String): Unit = store.remove(id)

        def keys(): Set[String] = store.keySet.toSet

    object InMemoryStateStore:
        def apply(): InMemoryStateStore = InMemoryStateStore(mutable.Map.empty)
