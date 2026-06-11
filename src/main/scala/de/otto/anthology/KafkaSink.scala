package de.otto.anthology

import com.typesafe.scalalogging.LazyLogging
import de.otto.anthology.BroadcastStage.broadcast
import de.otto.anthology.config.KafkaClusterSettings
import de.otto.anthology.kafka.AggregateIdSerializer
import de.otto.anthology.kafka.AggregateSerializer
import de.otto.anthology.kafka.ClusterName
import de.otto.anthology.kafka.ConsumerMap
import de.otto.anthology.kafka.Passthrough
import de.otto.anthology.kafka.TopicName
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

    extension (in: Flow[(Seq[(AggregateId, Option[Aggregate], Option[Headers])], Seq[Passthrough])])
        def emit(settings: KafkaSinkSettings): Unit =
            val credentials = settings.clusterSettings.credentials
            val baseSettings: ProducerSettings[AggregateId, Aggregate] =
                ProducerSettings.default
                    .bootstrapServers(settings.clusterSettings.config.bootstrapServers.split(",").map(_.trim)*)
                    .keySerializer(AggregateIdSerializer)
                    .valueSerializer(AggregateSerializer)
            val producerSettings =
                if credentials.isEmpty then baseSettings
                else
                    baseSettings
                        .property("security.protocol", "SASL_SSL")
                        .property("sasl.mechanism", "PLAIN")
                        .property(
                            "sasl.jaas.config",
                            s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${credentials(
                                    "username"
                                )}" password="${credentials("password")}";"""
                        )

            supervised:
                // setup channel for publishing
                val publishChannel = Channel.rendezvous[ProducerRecord[AggregateId, Aggregate]]
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
                var logCnt = 0
                // setup sink which sends incoming data to both publisher channel and committer channel
                in
                    .map: (payload, offsets) =>
                        val producerRecords =
                            payload.map: p =>
                                val recKey = p._1
                                val recValue = p._2.getOrElse(null.asInstanceOf[Aggregate])
                                val recHeaders = p._3.getOrElse(Collections.emptyList[Header]())
                                ProducerRecord(
                                    settings.config.topic.toString,
                                    null,
                                    recKey,
                                    recValue,
                                    recHeaders
                                ) // scalafix:ok
                        (producerRecords, offsets)
                    .map: (producerRecords, offsets) =>
                        if logCnt % 100 == 0 then logger.info("publishing and committing batch...")
                        logCnt += 1
                        producerRecords.foreach(publishChannel.send)
                        offsets.foreach(committerChannel.send)
                    .runDrain()

                (publishFork.join(), commitForkPerConsumer.join(), commitFork.join())

case class KafkaSinkConfig(cluster: ClusterName, topic: TopicName) derives ConfigReader

case class KafkaSinkSettings(
    config: KafkaSinkConfig,
    clusterSettings: KafkaClusterSettings,
    consumers: ConsumerMap
)
