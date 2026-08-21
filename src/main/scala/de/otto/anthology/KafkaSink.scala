package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.BroadcastStage.broadcast
import de.otto.anthology.SimpleLoggingCounterStage.count
import de.otto.anthology.SimpleProcessingTimeLogger.measure
import de.otto.anthology.config.AdditionalKafkaProperty
import de.otto.anthology.config.KafkaClusterSettings
import de.otto.anthology.config.asMap
import de.otto.anthology.kafka.ClusterName
import de.otto.anthology.kafka.ConsumerMap
import de.otto.anthology.kafka.MessageIdSerializer
import de.otto.anthology.kafka.MessageSerializer
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.kafka.TopicName
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.Headers
import ox.*
import ox.channels.Channel
import ox.flow.Flow
import ox.kafka.CommitPacket
import ox.kafka.KafkaDrain
import ox.kafka.ProducerSettings
import pureconfig.ConfigReader

import java.util.Collections

object KafkaSink extends LazyLogging:

    extension (in: Flow[(Seq[(MessageId, Option[Message], Option[Headers])], Seq[Passthrough])])
        def emitCodomainMessages(settings: KafkaSinkSettings): Unit =
            val additionalProps: Map[String, String] =
                settings.clusterSettings.additionalProperties ++ settings.config.additionalProducerPropertiesAsMap
            val baseSettings: ProducerSettings[MessageId, Message] =
                ProducerSettings.default
                    .bootstrapServers(settings.clusterSettings.config.bootstrapServers.split(",").map(_.trim)*)
                    .keySerializer(MessageIdSerializer)
                    .valueSerializer(MessageSerializer)
                    .property(ProducerConfig.BATCH_SIZE_CONFIG, 32768.toString) // 32kB
                    .property(ProducerConfig.LINGER_MS_CONFIG, 3000.toString) // 3s
                    .property(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
            val producerSettings: ProducerSettings[MessageId, Message] =
                additionalProps.foldLeft(baseSettings)((s, k2v) => s.property(k2v._1, k2v._2))

            supervised:
                // setup channel for publishing
                val publishChannel = Channel.rendezvous[ProducerRecord[MessageId, Message]]
                val publishFork =
                    fork:
                        Flow.fromSource(publishChannel)
                            .pipe(KafkaDrain.runPublish(producerSettings))

                // setup channel for committing - part I: one committing channel per consumer
                val (
                    commitChannelPerConsumer: Iterable[Channel[Passthrough]],
                    commitThunkPerConsumer: Iterable[() => Unit]
                ) =
                    settings.consumers
                        .map: (name, consumer) =>
                            val chan = Channel.rendezvous[Passthrough]
                            val commThunk =
                                () =>
                                    Flow.fromSource(chan)
                                        .filter(_.consumerName == name)
                                        .map(p => CommitPacket(p.record))
                                        .pipe(KafkaDrain.runCommit(consumer))
                            (chan, commThunk)
                        .unzip
                val commitForkPerConsumer = forkAll(commitThunkPerConsumer.toSeq)

                // setup channel for committing - part II: one main committing channel
                val committerChannel: Channel[Passthrough] = Channel.rendezvous[Passthrough]
                val commitFork =
                    fork:
                        Flow.fromSource(committerChannel).broadcast(commitChannelPerConsumer).runDrain()

                // setup sink which sends incoming data to both publisher channel and committer channel
                in
                    .map: (payload, offsets) =>
                        val producerRecords =
                            payload.map: p =>
                                val recKey = p._1
                                val recValue = p._2.getOrElse(null.asInstanceOf[Message]) // scalafix:ok
                                val recHeaders = p._3.getOrElse(Collections.emptyList[Header]())
                                ProducerRecord(
                                    settings.config.topic.toString,
                                    null,
                                    recKey,
                                    recValue,
                                    recHeaders
                                ) // scalafix:ok
                        (producerRecords, offsets)
                    .tap: (producerRecords, _) =>
                        if settings.logSentMessages.getOrElse(false) then
                            producerRecords.foreach: record =>
                                logger.info(
                                    s"Sending codomain message id=${record.key}, msg=${record.value}"
                                )
                    .logThroughput(settings)
                    .map:
                        measure("SinkPublish"): (producerRecords, offsets) =>
                            producerRecords.foreach(publishChannel.send)
                            offsets.foreach(committerChannel.send)
                    .mapStateful(0): (cnt, _) =>
                        if cnt % 1000 == 0 then
                            logger.info("Published and committed 1000 batches of codomain messages to Kafka")
                        (cnt + 1, ())
                    .runDrain()

                (publishFork.join(), commitForkPerConsumer.join(), commitFork.join())
        end emitCodomainMessages

    extension (flow: Flow[(Seq[ProducerRecord[MessageId, Message]], Seq[Passthrough])])
        def logThroughput(settings: KafkaSinkSettings)(using
            Ox
        ): Flow[(Seq[ProducerRecord[MessageId, Message]], Seq[Passthrough])] =
            if settings.logThroughput.getOrElse(false) then flow.count("codomain sink") else flow

case class KafkaSinkConfig(
    cluster: ClusterName,
    topic: TopicName,
    additionalProducerProperties: Option[Seq[AdditionalKafkaProperty]]
) derives ConfigReader:
    def additionalProducerPropertiesAsMap: Map[String, String] =
        additionalProducerProperties.getOrElse(Seq.empty).asMap

case class KafkaSinkSettings(
    config: KafkaSinkConfig,
    clusterSettings: KafkaClusterSettings,
    consumers: ConsumerMap,
    logSentMessages: Option[Boolean],
    logThroughput: Option[Boolean]
)
