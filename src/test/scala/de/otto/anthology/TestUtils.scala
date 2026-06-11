package de.otto.anthology

import com.fasterxml.jackson.databind.JsonNode
import de.otto.anthology.Aggregate
import de.otto.anthology.AggregateId
import de.otto.anthology.KafkaSourceConfig
import de.otto.anthology.kafka.ClusterName
import de.otto.anthology.kafka.ConsumerName
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.kafka.TopicName
import de.otto.anthology.statestore.StateStore
import org.apache.kafka.clients.consumer.ConsumerRecord
import ox.kafka.ReceivedMessage

import scala.collection.mutable

object TestUtils:

    def emptyKafkaConfig: KafkaSourceConfig = KafkaSourceConfig(ClusterName(""), TopicName(""), "")

    def mockedKafkaRecord(
        id: String,
        jsonNode: JsonNode,
        topic: String = "test-topic",
        partition: Int = 0,
        offset: Long = 0
    ): Passthrough =
        val record =
            new ConsumerRecord[AggregateId, Option[Aggregate]](
                topic,
                partition,
                offset,
                AggregateId(id),
                Some(Aggregate(jsonNode))
            )
        Passthrough(ReceivedMessage(record), ConsumerName("test-consumer"))

    def mockedEmptyKafkaRecord(
        id: String,
        topic: String = "test-topic",
        partition: Int = 0,
        offset: Long = 0
    ): Passthrough =
        val record =
            new ConsumerRecord[AggregateId, Option[Aggregate]](
                topic,
                partition,
                offset,
                AggregateId(id),
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
