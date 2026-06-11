package de.otto.anthology

import de.otto.anthology.JsonSupport.mapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import ox.channels.ActorRef
import ox.kafka.KafkaConsumerWrapper
import ox.kafka.ReceivedMessage
import pureconfig.ConfigReader

import java.util.Comparator
import java.util.Objects

object kafka:

    opaque type ClusterName = String
    object ClusterName:
        def apply(name: String): ClusterName = name
        given clusterNameConfigReader: ConfigReader[ClusterName] =
            ConfigReader[String].map(nameStr => ClusterName(nameStr))

    opaque type ConsumerName = String
    object ConsumerName:
        def apply(name: String): ConsumerName = name
        given consumerNameConfigReader: ConfigReader[ConsumerName] =
            ConfigReader[String].map(nameStr => ConsumerName(nameStr))

    opaque type TopicName = String
    object TopicName:
        def apply(name: String): TopicName = name
        given topicNameConfigReader: ConfigReader[TopicName] =
            ConfigReader[String].map(nameStr => TopicName(nameStr))

    object AggregateIdSerializer extends Serializer[AggregateId]:
        private val underlying: Serializer[String] = new StringSerializer
        override def serialize(topic: String, data: AggregateId): Array[Byte] =
            underlying.serialize(topic, data.toString)

    object AggregateIdDeserializer extends Deserializer[AggregateId]:
        private val underlying: Deserializer[String] = new StringDeserializer
        override def deserialize(topic: String, data: Array[Byte]): AggregateId =
            AggregateId(underlying.deserialize(topic, data))

    object AggregateSerializer extends Serializer[Aggregate]:
        override def serialize(topic: String, data: Aggregate): Array[Byte] =
            if Objects.isNull(data) then null else mapper.writeValueAsBytes(data.toJson) // scalafix:ok

    object AggregateDeserializer extends Deserializer[Option[Aggregate]]:
        override def deserialize(topic: String, data: Array[Byte]): Option[Aggregate] =
            if Objects.isNull(data) || data.isEmpty then None
            else Some(Aggregate(mapper.readTree(data)))

    case class Passthrough(record: ReceivedMessage[AggregateId, Option[Aggregate]], consumerName: ConsumerName)
    object Passthrough:
        given passthroughComparator: Comparator[Passthrough] =
            new Comparator:
                def compare(x: Passthrough, y: Passthrough): Int =
                    if x.record.topic != y.record.topic then x.record.topic.compareTo(y.record.topic)
                    else if x.record.partition != y.record.partition then x.record.partition.compare(y.record.partition)
                    else x.record.offset.compare(y.record.offset)

    type Consumer = ActorRef[KafkaConsumerWrapper[AggregateId, Option[Aggregate]]]
    type ConsumerMap = Map[ConsumerName, Consumer]
